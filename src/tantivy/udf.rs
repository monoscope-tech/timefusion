//! `text_match(col, 'query')` — returns BOOLEAN.
//!
//! Behavior: case-insensitive substring match across the column's string
//! representation. This is the *correctness fallback* used when the tantivy
//! prefilter isn't applied (e.g. on MemBuffer rows, or when the optimizer
//! couldn't prune via the index). The query language understood here is
//! intentionally tiny: any whitespace-separated token must appear (AND).
//! Tantivy at the prefilter layer can interpret a richer syntax; results
//! must remain a *superset* of what tantivy returns so post-filtering with
//! this UDF preserves correctness.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, StringArray, StringViewArray},
    datatypes::DataType,
};
use datafusion::{
    common::Result as DFResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

pub const TEXT_MATCH_NAME: &str = "text_match";

/// Minimum literal length accelerable on ngram3. Tantivy's 3-gram tokenizer
/// produces no tokens for shorter inputs, so a 2-char query would match every
/// doc (degenerate) — bail to scan.
pub const NGRAM_MIN_QUERY_LEN: usize = 3;

/// Conservative: only allow alnum, dot, dash, underscore, slash, `@`, and
/// space. Outside this allowlist we leave the predicate alone (the original
/// `=` / `LIKE` still applies — correctness preserved).
///
/// The reader no longer feeds routed literals to tantivy's `QueryParser`
/// except for `*`-suffixed prefix queries on raw/default fields (see
/// `reader::analyzed_conjunction_query`), so the allowlist is now about
/// keeping ROUTING conservative rather than about parser safety — a char the
/// analyzer treats unexpectedly would still only ever narrow the prefilter.
/// Colon stays excluded for the remaining parser path (field-delimiter syntax).
pub fn is_tantivy_safe_term_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '.' | '-' | '_' | ' ' | '/' | '@')
}

/// Stricter than `is_tantivy_safe_term_char`, for exact `=` routing: the
/// literal is fed to tantivy's `QueryParser` as a term against a raw-tokenized
/// field (one token = the whole value). Whitespace AND-splits and punctuation
/// is query syntax, either of which silently empties the hit set — restrict to
/// chars that pass through unchanged (trace/span ids, UUIDs, enums).
pub fn is_eq_term_safe(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '-' | '_')
}

/// Decide which Tantivy query form a SQL LIKE pattern maps to.
///
/// `allow_substring=false` (raw/default tokenizer):
///   - `'foo'`     → term `foo`
///   - `'foo%'`    → prefix `foo*`
///   - `'%foo'`, `'%foo%'`, embedded `%` → unsupported (None)
///
/// `allow_substring=true` (ngram3 tokenizer):
///   - `'%foo'` / `'%foo%'` → term `foo` (n-gram match by tantivy)
///   - Embedded `%` between literal chars (e.g. `'a%b'`) → unsupported
///
/// `_` (single-char wildcard) is never accelerable. Returns None.
pub fn classify_like_pattern(pat: &str, escape: Option<char>, allow_substring: bool) -> Option<String> {
    let esc = escape.unwrap_or('\\');
    let mut it = pat.chars().peekable();
    let leading_wildcard = it.next_if_eq(&'%').is_some();
    let mut out = String::new();
    let mut trailing_wildcard = false;
    while let Some(c) = it.next() {
        let lit = match c {
            c if c == esc => it.next()?, // trailing escape → bail
            '_' => return None,
            // Only the leading-or-trailing-only wildcard forms are handled:
            // `'a%b'` would need positional ranking tantivy can't trivially give.
            '%' if it.peek().is_none() => {
                trailing_wildcard = true;
                break;
            }
            '%' => return None,
            other => other,
        };
        if !is_tantivy_safe_term_char(lit) {
            return None;
        }
        out.push(lit);
    }
    if out.is_empty() {
        return None;
    }
    Some(match (leading_wildcard, trailing_wildcard) {
        // Plain exact / prefix / suffix / infix matches.
        (false, false) => out,      // 'foo'
        (false, true) => out + "*", // 'foo%' (prefix)
        // Suffix-only and infix forms only meaningful on ngram3; for raw/
        // default tokenizers we'd be sending tantivy a query that matches
        // the substring as a whole token (it won't). Bail.
        (true, false) | (true, true) if !allow_substring => return None,
        (true, _) => out, // ngram3 will trigram-match the substring
    })
}

/// POSIX-regex metacharacters. A pattern containing any of these *unescaped*
/// is not a plain substring and is never routed. Deliberately the same set
/// monoscope's `escapeRegex` escapes (`Pkg/DeriveUtils.hs`), so a KQL
/// has/contains term round-trips exactly; anything else bails.
const REGEX_META: &str = ".^$*+?()[]{}|\\";

/// Decode a `~` / `~*` pattern that is a PLAIN LITERAL SUBSTRING into that
/// substring, or `None` when the pattern uses any regex feature.
///
/// `\X` unescapes to `X` only for X in [`REGEX_META`] — the exact convention
/// monoscope emits. Any other backslash escape (`\d`, `\m`, `\y`, `\w`, …) is
/// a character class or word-boundary assertion, not a literal, so it bails.
/// Unescaped metacharacters bail. The decoded literal must also survive
/// tantivy's `QueryParser` unchanged ([`is_tantivy_safe_term_char`]).
///
/// Anchors (`^foo`, `foo$` — monoscope's startswith/endswith) contain `^`/`$`
/// and therefore bail: prefix/suffix routing over a 3-gram field needs its own
/// correctness argument and is out of scope here.
pub fn regex_literal_substring(pat: &str) -> Option<String> {
    let mut out = String::new();
    let mut it = pat.chars();
    while let Some(c) = it.next() {
        let lit = match c {
            // trailing backslash / non-meta escape (`\d`, `\y`, …) → not a literal
            '\\' => it.next().filter(|n| REGEX_META.contains(*n))?,
            c if REGEX_META.contains(c) => return None,
            c => c,
        };
        if !is_tantivy_safe_term_char(lit) {
            return None;
        }
        out.push(lit);
    }
    (!out.is_empty()).then_some(out)
}

/// Runtime classification of a DEFERRED (placeholder-routed) `text_match`.
/// The rewriter couldn't validate a `$N` at plan time, so it tagged the call
/// with the predicate kind + tokenizer; once parameter substitution turns the
/// placeholder into a literal, this reproduces exactly the plan-time gates.
/// `kind`: `"eq"` | `"like:<tokenizer>"` | `"ilike:<tokenizer>"`.
/// `None` = not accelerable → the call is opaque to the prefilter (the
/// original predicate still post-filters).
pub fn classify_deferred(kind: &str, value: &str) -> Option<String> {
    use crate::tantivy::{NGRAM3_TOKENIZER, RAW_TOKENIZER};
    if kind == "eq" {
        return (!value.is_empty() && value.chars().all(is_eq_term_safe)).then(|| value.to_string());
    }
    let (form, tok) = kind.split_once(':')?;
    match form {
        "ilike" if tok == RAW_TOKENIZER => return None, // case-sensitive single token can't serve ILIKE
        "like" | "ilike" => {}
        _ => return None,
    }
    let allow_substring = tok == NGRAM3_TOKENIZER;
    let q = classify_like_pattern(value, None, allow_substring)?;
    (!allow_substring || q.chars().filter(|c| *c != '*').count() >= NGRAM_MIN_QUERY_LEN).then_some(q)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TextMatchUdf {
    sig: Signature,
}

impl Default for TextMatchUdf {
    fn default() -> Self {
        // 2-arg: plan-time-classified query. 3-arg: deferred placeholder
        // routing — (col, $N, kind); the 3rd arg is consumed by the scan-side
        // collector, not by row evaluation.
        Self { sig: Signature::one_of(vec![TypeSignature::Any(2), TypeSignature::Any(3)], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for TextMatchUdf {
    fn name(&self) -> &str {
        TEXT_MATCH_NAME
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let arrs = args.args.iter().map(|c| c.to_array(n)).collect::<DFResult<Vec<ArrayRef>>>()?;
        let col_str = string_extractor(&arrs[0]);
        let pat_str = string_extractor(&arrs[1]);
        // 3-arg deferred calls carry the RAW predicate value + kind; their
        // row-eval must reproduce the ORIGINAL predicate's semantics (a
        // superset of it), not tantivy token containment — substring-matching
        // a raw LIKE pattern silently dropped rows for `_`/embedded-`%`.
        let kind: Option<String> = arrs.get(2).filter(|a| !a.is_empty()).and_then(|a| string_extractor(a)(0));
        let out: BooleanArray = (0..n)
            .map(|i| {
                Some(match (col_str(i), pat_str(i)) {
                    (Some(haystack), Some(needle)) => match kind.as_deref() {
                        Some(k) => deferred_row_matches(k, &needle, &haystack),
                        // 2-arg: plan-time-classified tantivy syntax
                        // (`'foo*'` prefix, `'foo'` substring on ngram3);
                        // strip wildcards and require token containment.
                        None => {
                            let h_low = haystack.to_lowercase();
                            needle
                                .to_lowercase()
                                .split_whitespace()
                                .map(|tok| tok.trim_matches(|c: char| c == '*' || c == '?'))
                                .all(|tok| !tok.is_empty() && h_low.contains(tok))
                        }
                    },
                    _ => false,
                })
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

/// Row-level evaluation of a DEFERRED (3-arg) text_match: a SUPERSET of the
/// original predicate. `eq` → case-insensitive containment (⊇ `=`); `like`/
/// `ilike` → case-insensitive SQL LIKE with `%`/`_`/default `\` escape
/// (case-folding makes it a superset of case-sensitive LIKE; the original
/// predicate re-filters exactly).
fn deferred_row_matches(kind: &str, value: &str, haystack: &str) -> bool {
    match kind {
        "eq" => haystack.to_lowercase().contains(&value.to_lowercase()),
        _ => like_match_ci(value, haystack),
    }
}

/// Case-insensitive SQL LIKE. Classic two-pointer glob with `%` backtracking;
/// `_` matches exactly one char; `\` escapes the next char.
fn like_match_ci(pattern: &str, text: &str) -> bool {
    enum Tok {
        Percent,
        One,
        Lit(char),
    }
    let lowered = pattern.to_lowercase();
    let mut chars = lowered.chars();
    let toks: Vec<Tok> = std::iter::from_fn(|| {
        chars.next().map(|c| match c {
            '\\' => chars.next().map(Tok::Lit), // trailing escape is dropped
            '%' => Some(Tok::Percent),
            '_' => Some(Tok::One),
            other => Some(Tok::Lit(other)),
        })
    })
    .flatten()
    .collect();
    // Backtracking two-pointer glob: kept imperative — the `%` restart point
    // is state no iterator adapter expresses without re-walking the text.
    let t: Vec<char> = text.to_lowercase().chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        let step = match toks.get(pi) {
            Some(Tok::One) => true,
            Some(Tok::Lit(c)) => *c == t[ti],
            _ => false,
        };
        if step {
            pi += 1;
            ti += 1;
        } else if matches!(toks.get(pi), Some(Tok::Percent)) {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            star = Some((sp, st + 1));
            (pi, ti) = (sp + 1, st + 1);
        } else {
            return false;
        }
    }
    toks[pi..].iter().all(|k| matches!(k, Tok::Percent))
}

fn string_extractor(arr: &ArrayRef) -> Box<dyn Fn(usize) -> Option<String> + '_> {
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().expect("Utf8 array");
            Box::new(move |i| (!a.is_null(i)).then(|| a.value(i).to_string()))
        }
        DataType::Utf8View => {
            let a = arr.as_any().downcast_ref::<StringViewArray>().expect("Utf8View array");
            Box::new(move |i| (!a.is_null(i)).then(|| a.value(i).to_string()))
        }
        // Variant Struct{metadata,value}: render each row to canonical JSON text
        // via the SAME serializer the tantivy index and the LIKE-coercion path
        // use (`builder::variant_to_text`), so text_match's row-eval agrees
        // byte-for-byte with them and stays a superset of the real predicate.
        // Without this, predicates on Variant columns (e.g. `body LIKE '%x%'`,
        // rewritten to `… AND text_match(body,'x')`) silently never match.
        // Decoded lazily per row (only when the closure is called).
        DataType::Struct(_) if crate::schema::is_variant_type(arr.data_type()) => {
            Box::new(move |i| crate::tantivy::variant_to_text(arr, i, false).ok().flatten())
        }
        // Anything else — degrade to never-match.
        _ => Box::new(|_| None),
    }
}

pub fn text_match_udf() -> ScalarUDF {
    ScalarUDF::from(TextMatchUdf::default())
}

/// Detect a `text_match(col, 'q'[, kind])` predicate and extract its column
/// name and tantivy query. 2-arg calls carry a plan-time-classified query;
/// 3-arg calls were routed on a `$N` placeholder and are classified HERE,
/// after parameter substitution turned the placeholder into a literal.
/// `None` = not a routable call (the collector treats it as opaque).
pub fn extract_text_match(expr: &datafusion::logical_expr::Expr) -> Option<TextMatchPred> {
    use datafusion::{logical_expr::Expr, scalar::ScalarValue};
    fn utf8_lit(e: &Expr) -> Option<String> {
        match e {
            Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
            _ => None,
        }
    }
    let Expr::ScalarFunction(sf) = expr else { return None };
    if sf.func.name() != TEXT_MATCH_NAME {
        return None;
    }
    let Some(Expr::Column(c)) = sf.args.first() else { return None };
    let query = match sf.args.as_slice() {
        [_, q] => utf8_lit(q)?,
        // a `$N` still un-substituted fails `utf8_lit` → opaque
        [_, value, kind] => classify_deferred(&utf8_lit(kind)?, &utf8_lit(value)?)?,
        _ => return None,
    };
    Some(TextMatchPred { column: c.name.clone(), query })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextMatchPred {
    pub column: String,
    pub query: String,
}

/// Boolean structure of the routable `text_match` predicates in a filter
/// tree. Evaluated per tantivy index (And→Must, Or→Should) and against the
/// MemBuffer bucket indexes, so AND intersects and OR unions *inside* the
/// engine rather than by combining per-predicate id sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredNode {
    And(Vec<PredNode>),
    Or(Vec<PredNode>),
    Leaf(TextMatchPred),
}

impl PredNode {
    /// Every column referenced by a leaf (for field-coverage checks).
    pub fn columns(&self) -> Vec<&str> {
        match self {
            PredNode::Leaf(p) => vec![p.column.as_str()],
            PredNode::And(kids) | PredNode::Or(kids) => kids.iter().flat_map(|k| k.columns()).collect(),
        }
    }

    /// Conjunction of flat predicates (legacy shape used by tests/tools).
    pub fn from_preds(preds: &[TextMatchPred]) -> Option<PredNode> {
        combine(true, preds.iter().cloned().map(PredNode::Leaf))
    }
}

/// Fold children into one `And`/`Or` node, flattening same-kind nesting (for
/// readability of the compiled query) and collapsing the 0/1-child cases.
fn combine(and: bool, nodes: impl IntoIterator<Item = PredNode>) -> Option<PredNode> {
    let kids: Vec<PredNode> = nodes
        .into_iter()
        .flat_map(|n| match n {
            PredNode::And(inner) if and => inner,
            PredNode::Or(inner) if !and => inner,
            other => vec![other],
        })
        .collect();
    match kids.len() {
        0 => None,
        1 => kids.into_iter().next(),
        _ => Some(if and { PredNode::And(kids) } else { PredNode::Or(kids) }),
    }
}

/// Result of translating one expr subtree.
/// `node`: the routable prefilter structure found inside, if any.
/// `complete`: the subtree's TRUE match set is fully covered by `node`
/// (i.e. `node`'s hits ⊇ subtree's matches). Required for OR-union
/// soundness: a branch without complete coverage would make the union a
/// non-superset and silently drop that branch's rows (2026-06-16 dashboard
/// bug: `(kind='server' OR name='...')` returned 0 from Delta).
struct NodeRes {
    node: Option<PredNode>,
    complete: bool,
}

/// Extract the routable prefilter tree from pushed-down filters (implicitly
/// AND-ed). Returns `None` when nothing routable was found. Soundness rules:
/// - `text_match` leaf: complete (rewriter guarantees hits ⊇ original
///   predicate's matches).
/// - AND: prefilter = conjunction of whichever children are routable
///   (a superset of the AND's matches since each child's prefilter is a
///   superset of its own matches). Complete if ANY child is complete —
///   the AND's matches ⊆ that child's matches ⊆ its prefilter. This is what
///   makes `orig = 'x' AND text_match(...)` (the rewriter's additive shape)
///   a complete OR branch.
/// - OR: routable only if ALL children are routable AND complete; else the
///   whole node is opaque (no prefilter from inside it may be used).
/// - anything else: opaque, incomplete.
pub fn collect_text_match_tree(filters: &[datafusion::logical_expr::Expr]) -> Option<PredNode> {
    combine(true, filters.iter().filter_map(|f| expr_node(f).node))
}

fn expr_node(e: &datafusion::logical_expr::Expr) -> NodeRes {
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    if let Some(p) = extract_text_match(e) {
        return NodeRes { node: Some(PredNode::Leaf(p)), complete: true };
    }
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
            let (a, b) = (expr_node(left), expr_node(right));
            NodeRes { node: combine(true, [a.node, b.node].into_iter().flatten()), complete: a.complete || b.complete }
        }
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Or, right }) => {
            let (a, b) = (expr_node(left), expr_node(right));
            match (a.node, b.node, a.complete && b.complete) {
                (Some(an), Some(bn), true) => NodeRes { node: combine(false, [an, bn]), complete: true },
                _ => NodeRes { node: None, complete: false },
            }
        }
        _ => NodeRes { node: None, complete: false },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_like_row_eval_is_a_superset_of_sql_like() {
        // `_` and embedded `%` — the shapes the old substring row-eval dropped.
        assert!(like_match_ci("a_c", "abc"));
        assert!(!like_match_ci("a_c", "abbc"));
        assert!(like_match_ci("foo%bar", "fooXbar"));
        assert!(like_match_ci("foo%bar", "foobar"));
        assert!(!like_match_ci("foo%bar", "fooba"));
        assert!(like_match_ci("%user_id%", "xuserXidz"));
        assert!(like_match_ci("%foo%", "afoob"));
        assert!(like_match_ci("foo", "FOO"), "case-insensitive superset of LIKE");
        assert!(!like_match_ci("foo", "food"), "no wildcard = exact length");
        assert!(like_match_ci("a\\_c", "a_c"), "escaped underscore is literal");
        assert!(!like_match_ci("a\\_c", "abc"));
        assert!(like_match_ci("%", ""));
        assert!(!like_match_ci("_", ""));

        assert!(deferred_row_matches("eq", "abc", "xxabcyy"), "eq → containment superset");
        assert!(deferred_row_matches("like:tf_ngram3", "%a_b%", "zzaXbzz"));
        assert!(!deferred_row_matches("like:tf_ngram3", "%a_b%", "zzabzz"));
    }

    #[test]
    fn regex_literal_substring_accepts_only_escaped_literals() {
        // monoscope's `escapeRegex` output round-trips.
        assert_eq!(regex_literal_substring("runServer"), Some("runServer".into()));
        assert_eq!(regex_literal_substring("svc\\.user-api"), Some("svc.user-api".into()));
        assert_eq!(regex_literal_substring("GET /v1/users"), Some("GET /v1/users".into()));
        // Any live regex feature bails.
        for p in ["run.*", "^foo", "foo$", "a|b", "f(o)o", "a[bc]", "x{2}", "\\d+", "\\yword\\y", "\\w", "trailing\\", "", "a\\+b"] {
            assert_eq!(regex_literal_substring(p), None, "{p:?} must not decode to a literal");
        }
    }
}
