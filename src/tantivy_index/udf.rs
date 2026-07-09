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
    array::{Array, ArrayRef, BooleanBuilder, StringArray, StringViewArray},
    datatypes::DataType,
};
use datafusion::{
    common::Result as DFResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
};

pub const TEXT_MATCH_NAME: &str = "text_match";

/// Minimum literal length accelerable on ngram3. Tantivy's 3-gram tokenizer
/// produces no tokens for shorter inputs, so a 2-char query would match every
/// doc (degenerate) — bail to scan.
pub const NGRAM_MIN_QUERY_LEN: usize = 3;

/// Conservative: only allow alnum, dot, dash, underscore, slash, `@`, and
/// space. Colon is deliberately *excluded* — Tantivy QueryParser treats it as
/// field-delimiter syntax, and many other ASCII punctuation chars are query
/// syntax; outside this allowlist we leave the predicate alone (the original
/// `=` / `LIKE` still applies — correctness preserved).
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
    let chars: Vec<char> = pat.chars().collect();
    let total = chars.len();
    if total == 0 {
        return None;
    }
    let mut out = String::new();
    let mut i = 0;
    let mut leading_wildcard = false;
    let mut trailing_wildcard = false;
    // Detect leading %
    if chars[0] == '%' {
        leading_wildcard = true;
        i = 1;
    }
    while i < total {
        let c = chars[i];
        if c == esc {
            // Next char is literal.
            i += 1;
            if i >= total {
                return None; // trailing escape
            }
            let n = chars[i];
            if !is_tantivy_safe_term_char(n) {
                return None;
            }
            out.push(n);
            i += 1;
            continue;
        }
        if c == '_' {
            return None;
        }
        if c == '%' {
            if i + 1 == total {
                trailing_wildcard = true;
                break;
            }
            // Embedded %: only the leading-or-trailing-only forms are
            // handled here. `'a%b'` would need positional ranking that
            // tantivy can't trivially give us. Bail.
            return None;
        }
        if !is_tantivy_safe_term_char(c) {
            return None;
        }
        out.push(c);
        i += 1;
    }
    if out.is_empty() {
        return None;
    }
    Some(match (leading_wildcard, trailing_wildcard) {
        // Plain exact / prefix / suffix / infix matches.
        (false, false) => out,                // 'foo'
        (false, true) => format!("{}*", out), // 'foo%' (prefix)
        // Suffix-only and infix forms only meaningful on ngram3; for raw/
        // default tokenizers we'd be sending tantivy a query that matches
        // the substring as a whole token (it won't). Bail.
        (true, false) | (true, true) if !allow_substring => return None,
        (true, _) => out, // ngram3 will trigram-match the substring
    })
}

/// Runtime classification of a DEFERRED (placeholder-routed) `text_match`.
/// The rewriter couldn't validate a `$N` at plan time, so it tagged the call
/// with the predicate kind + tokenizer; once parameter substitution turns the
/// placeholder into a literal, this reproduces exactly the plan-time gates.
/// `kind`: `"eq"` | `"like:<tokenizer>"` | `"ilike:<tokenizer>"`.
/// `None` = not accelerable → the call is opaque to the prefilter (the
/// original predicate still post-filters).
pub fn classify_deferred(kind: &str, value: &str) -> Option<String> {
    use crate::tantivy_index::schema::{NGRAM3_TOKENIZER, RAW_TOKENIZER};
    if kind == "eq" {
        return (!value.is_empty() && value.chars().all(is_eq_term_safe)).then(|| value.to_string());
    }
    let (form, tok) = kind.split_once(':')?;
    let ci = match form {
        "like" => false,
        "ilike" => true,
        _ => return None,
    };
    if ci && tok == RAW_TOKENIZER {
        return None; // case-sensitive single token can't serve ILIKE
    }
    let allow_substring = tok == NGRAM3_TOKENIZER;
    let q = classify_like_pattern(value, None, allow_substring)?;
    if allow_substring && q.chars().filter(|c| *c != '*').count() < NGRAM_MIN_QUERY_LEN {
        return None;
    }
    Some(q)
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
        Self {
            sig: Signature::one_of(
                vec![datafusion::logical_expr::TypeSignature::Any(2), datafusion::logical_expr::TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
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
        let arrs = args
            .args
            .into_iter()
            .map(|c| match c {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows).expect("scalar→array"),
            })
            .collect::<Vec<ArrayRef>>();
        let col = &arrs[0];
        let pat = &arrs[1];
        let n = args.number_rows;
        let mut b = BooleanBuilder::with_capacity(n);
        let col_str: Box<dyn Fn(usize) -> Option<String>> = string_extractor(col);
        let pat_str: Box<dyn Fn(usize) -> Option<String>> = string_extractor(pat);
        // 3-arg deferred calls carry the RAW predicate value + kind; their
        // row-eval must reproduce the ORIGINAL predicate's semantics (a
        // superset of it), not tantivy token containment — substring-matching
        // a raw LIKE pattern silently dropped rows for `_`/embedded-`%`.
        let kind: Option<String> = arrs.get(2).and_then(|a| string_extractor(a)(0));
        for i in 0..n {
            match (col_str(i), pat_str(i)) {
                (Some(haystack), Some(needle)) => {
                    let ok = match kind.as_deref() {
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
                    };
                    b.append_value(ok);
                }
                _ => b.append_value(false),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

/// Row-level evaluation of a DEFERRED (3-arg) text_match: a SUPERSET of the
/// original predicate. `eq` → case-insensitive containment (⊇ `=`); `like`/
/// `ilike` → case-insensitive SQL LIKE with `%`/`_`/default `\` escape
/// (case-folding makes it a superset of case-sensitive LIKE; the original
/// predicate re-filters exactly).
fn deferred_row_matches(kind: &str, value: &str, haystack: &str) -> bool {
    if kind == "eq" {
        return haystack.to_lowercase().contains(&value.to_lowercase());
    }
    like_match_ci(value, haystack)
}

/// Case-insensitive SQL LIKE. Classic two-pointer glob with `%` backtracking;
/// `_` matches exactly one char; `\` escapes the next char.
fn like_match_ci(pattern: &str, text: &str) -> bool {
    enum Tok {
        Percent,
        One,
        Lit(char),
    }
    let mut toks: Vec<Tok> = Vec::new();
    let mut it = pattern.to_lowercase().chars().collect::<Vec<_>>().into_iter();
    while let Some(c) = it.next() {
        match c {
            '\\' => {
                if let Some(n) = it.next() {
                    toks.push(Tok::Lit(n));
                }
            }
            '%' => toks.push(Tok::Percent),
            '_' => toks.push(Tok::One),
            other => toks.push(Tok::Lit(other)),
        }
    }
    let t: Vec<char> = text.to_lowercase().chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0usize);
    while ti < t.len() {
        let step = pi < toks.len()
            && match &toks[pi] {
                Tok::One => true,
                Tok::Lit(c) => *c == t[ti],
                Tok::Percent => false,
            };
        if step {
            pi += 1;
            ti += 1;
        } else if pi < toks.len() && matches!(toks[pi], Tok::Percent) {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < toks.len() && matches!(toks[pi], Tok::Percent) {
        pi += 1;
    }
    pi == toks.len()
}

fn string_extractor(arr: &ArrayRef) -> Box<dyn Fn(usize) -> Option<String> + '_> {
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Box::new(move |i| if a.is_null(i) { None } else { Some(a.value(i).to_string()) })
        }
        DataType::Utf8View => {
            let a = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
            Box::new(move |i| if a.is_null(i) { None } else { Some(a.value(i).to_string()) })
        }
        // Variant Struct{metadata,value}: render each row to canonical JSON text
        // via the SAME serializer the tantivy index and the LIKE-coercion path
        // use (`builder::variant_to_text`), so text_match's row-eval agrees
        // byte-for-byte with them and stays a superset of the real predicate.
        // Without this, predicates on Variant columns (e.g. `body LIKE '%x%'`,
        // rewritten to `… AND text_match(body,'x')`) silently never match.
        // Decoded lazily per row (only when the closure is called).
        DataType::Struct(_) if crate::schema_loader::is_variant_type(arr.data_type()) => {
            Box::new(move |i| crate::tantivy_index::builder::variant_to_text(arr, i, false).ok().flatten())
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
    let col = match sf.args.first()? {
        Expr::Column(c) => c.name.clone(),
        _ => return None,
    };
    match sf.args.len() {
        2 => Some(TextMatchPred {
            column: col,
            query:  utf8_lit(&sf.args[1])?,
        }),
        3 => {
            let value = utf8_lit(&sf.args[1])?; // still a placeholder pre-substitution → opaque
            let kind = utf8_lit(&sf.args[2])?;
            Some(TextMatchPred {
                column: col,
                query:  classify_deferred(&kind, &value)?,
            })
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextMatchPred {
    pub column: String,
    pub query:  String,
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
        match preds.len() {
            0 => None,
            1 => Some(PredNode::Leaf(preds[0].clone())),
            _ => Some(PredNode::And(preds.iter().cloned().map(PredNode::Leaf).collect())),
        }
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
    node:     Option<PredNode>,
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
    let kids: Vec<PredNode> = filters.iter().filter_map(|f| expr_node(f).node).collect();
    match kids.len() {
        0 => None,
        1 => Some(kids.into_iter().next().expect("len 1")),
        _ => Some(PredNode::And(kids)),
    }
}

fn expr_node(e: &datafusion::logical_expr::Expr) -> NodeRes {
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    if let Some(p) = extract_text_match(e) {
        return NodeRes {
            node:     Some(PredNode::Leaf(p)),
            complete: true,
        };
    }
    match e {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let (a, b) = (expr_node(left), expr_node(right));
            let mut kids: Vec<PredNode> = Vec::new();
            for n in [a.node, b.node].into_iter().flatten() {
                // Flatten nested Ands for readability of the compiled query.
                match n {
                    PredNode::And(inner) => kids.extend(inner),
                    other => kids.push(other),
                }
            }
            let node = match kids.len() {
                0 => None,
                1 => Some(kids.into_iter().next().expect("len 1")),
                _ => Some(PredNode::And(kids)),
            };
            NodeRes {
                node,
                complete: a.complete || b.complete,
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Or, right }) => {
            let (a, b) = (expr_node(left), expr_node(right));
            match (a.node, b.node, a.complete && b.complete) {
                (Some(an), Some(bn), true) => {
                    let mut kids = Vec::new();
                    for n in [an, bn] {
                        match n {
                            PredNode::Or(inner) => kids.extend(inner),
                            other => kids.push(other),
                        }
                    }
                    NodeRes {
                        node:     Some(PredNode::Or(kids)),
                        complete: true,
                    }
                }
                _ => NodeRes {
                    node:     None,
                    complete: false,
                },
            }
        }
        _ => NodeRes {
            node:     None,
            complete: false,
        },
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
}
