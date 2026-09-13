//! `text_match(col, 'query')` — returns BOOLEAN.
//!
//! Case-insensitive AND-token fallback for rows not covered by an index.
//! Its matches must remain a superset of the Tantivy prefilter.

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

/// Minimum literal length accepted by the trigram index.
pub const NGRAM_MIN_QUERY_LEN: usize = 3;

/// Accepts only characters whose analyzer behavior is safe for routing.
pub fn is_tantivy_safe_term_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '.' | '-' | '_' | ' ' | '/' | '@')
}

/// Accepts exact terms that pass through the raw query parser unchanged.
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
    // Imperative on purpose: an invalid char bails the whole function while a
    // trailing '%' only stops the loop — one iterator adapter can't do both.
    while let Some(c) = it.next() {
        let lit = match c {
            c if c == esc => it.next()?, // trailing escape → bail
            '_' => return None,
            // Only leading/trailing wildcards are routable; `'a%b'` needs
            // positional ranking tantivy can't give.
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
        (false, false) => out,      // 'foo'
        (false, true) => out + "*", // 'foo%' (prefix)
        // Leading-wildcard forms only work on ngram3; other tokenizers would
        // match the substring as a whole token, i.e. never.
        (true, false) | (true, true) if !allow_substring => return None,
        (true, _) => out,
    })
}

/// Regex metacharacters recognised by [`regex_literal_substring`].
const REGEX_META: &str = ".^$*+?()[]{}|\\";

/// Decode a `~` / `~*` pattern that is a plain literal substring into that
/// substring, or `None` when the pattern uses any regex feature.
///
/// `\X` unescapes to `X` only for X in [`REGEX_META`]; any other escape (`\d`,
/// `\y`, `\w`, …) is a class/assertion and bails, as do unescaped
/// metacharacters (including the anchors `^`/`$`). The decoded literal must
/// also survive tantivy's `QueryParser` unchanged ([`is_tantivy_safe_term_char`]).
pub fn regex_literal_substring(pat: &str) -> Option<String> {
    let mut it = pat.chars();
    let out = std::iter::from_fn(move || {
        let c = it.next()?;
        Some(
            match c {
                // trailing backslash / non-meta escape (`\d`, `\y`, …) → not a literal
                '\\' => it.next().filter(|n| REGEX_META.contains(*n)),
                c if REGEX_META.contains(c) => None,
                c => Some(c),
            }
            .filter(|c| is_tantivy_safe_term_char(*c)),
        )
    })
    .collect::<Option<String>>()?;
    (!out.is_empty()).then_some(out)
}

/// Classify a deferred (placeholder-routed) `text_match` once the `$N` has been
/// substituted, applying the same gates the plan-time path applies.
/// `kind`: `"eq"` | `"like:<tokenizer>"` | `"ilike:<tokenizer>"`.
/// `None` = not accelerable; the call is opaque to the prefilter.
pub fn classify_deferred(kind: &str, value: &str) -> Option<String> {
    use crate::tantivy::{NGRAM3_TOKENIZER, RAW_TOKENIZER};
    let Some((form, tok)) = kind.split_once(':') else {
        return (kind == "eq" && !value.is_empty() && value.chars().all(is_eq_term_safe)).then(|| value.to_string());
    };
    let allow_substring = match (form, tok) {
        ("ilike", t) if t == RAW_TOKENIZER => return None, // case-sensitive single token can't serve ILIKE
        ("like" | "ilike", t) => t == NGRAM3_TOKENIZER,
        _ => return None,
    };
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
        // routing — (col, $N, kind).
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
        // 3-arg deferred calls carry the RAW predicate value + kind, so their
        // row-eval must reproduce the original predicate's semantics (as a
        // superset), not tantivy token containment.
        let kind: Option<String> = arrs.get(2).filter(|a| !a.is_empty()).and_then(|a| string_extractor(a)(0));
        let out: BooleanArray = (0..n)
            .map(|i| {
                Some(col_str(i).zip(pat_str(i)).is_some_and(|(haystack, needle)| {
                    kind.as_deref().map_or_else(|| tantivy_tokens_contained(&needle, &haystack), |k| deferred_row_matches(k, &needle, &haystack))
                }))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

/// Row eval of a 2-arg (plan-time-classified) text_match: the query is tantivy
/// syntax (`'foo*'` prefix, `'foo'` substring on ngram3), so strip wildcards and
/// require every token to be contained, case-insensitively.
fn tantivy_tokens_contained(query: &str, haystack: &str) -> bool {
    let h_low = haystack.to_lowercase();
    query.to_lowercase().split_whitespace().map(|tok| tok.trim_matches(|c: char| c == '*' || c == '?')).all(|tok| !tok.is_empty() && h_low.contains(tok))
}

/// Row eval of a deferred (3-arg) text_match; must stay a SUPERSET of the
/// original predicate, which re-filters exactly. `eq` → case-insensitive
/// containment; `like`/`ilike` → case-insensitive SQL LIKE.
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
        Some(match chars.next()? {
            '\\' => Tok::Lit(chars.next()?), // trailing escape is dropped
            '%' => Tok::Percent,
            '_' => Tok::One,
            other => Tok::Lit(other),
        })
    })
    .collect();
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
    fn strs<'a, A: Array + 'static>(arr: &'a ArrayRef, what: &str, val: impl Fn(&'a A, usize) -> &'a str + 'a) -> Box<dyn Fn(usize) -> Option<String> + 'a> {
        let a = arr.as_any().downcast_ref::<A>().expect(what);
        Box::new(move |i| (!a.is_null(i)).then(|| val(a, i).to_string()))
    }
    match arr.data_type() {
        DataType::Utf8 => strs(arr, "Utf8 array", |a: &StringArray, i| a.value(i)),
        DataType::Utf8View => strs(arr, "Utf8View array", |a: &StringViewArray, i| a.value(i)),
        // Variant Struct{metadata,value}: render lazily per row via the SAME
        // serializer the tantivy index uses, or row-eval disagrees with the
        // index and predicates on Variant columns silently never match.
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
/// name and tantivy query; 3-arg (placeholder-routed) calls are classified
/// here. `None` = not routable, and the collector treats it as opaque.
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

/// Boolean structure of the routable `text_match` predicates in a filter tree,
/// evaluated inside the tantivy/MemBuffer indexes (And→Must, Or→Should).
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

    /// Conjunction of flat predicates.
    pub fn from_preds(preds: &[TextMatchPred]) -> Option<PredNode> {
        combine(true, preds.iter().cloned().map(PredNode::Leaf))
    }
}

/// Fold children into one `And`/`Or` node, flattening same-kind nesting and
/// collapsing the 0/1-child cases.
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
/// `complete`: `node`'s hits ⊇ the subtree's matches. Required for OR-union
/// soundness — an incomplete branch makes the union a non-superset and
/// silently drops that branch's rows.
#[derive(Default)]
struct NodeRes {
    node: Option<PredNode>,
    complete: bool,
}

/// Extract the routable prefilter tree from pushed-down filters (implicitly
/// AND-ed). Returns `None` when nothing routable was found. Soundness rules:
/// - `text_match` leaf: complete (the rewriter guarantees hits ⊇ matches).
/// - AND: conjunction of whichever children are routable; complete if ANY
///   child is complete.
/// - OR: routable only if ALL children are routable AND complete; otherwise
///   the whole node is opaque and nothing inside it may be used.
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
                _ => NodeRes::default(),
            }
        }
        _ => NodeRes::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_case::test_case("a_c", "abc" => true)]
    #[test_case::test_case("a_c", "abbc" => false)]
    #[test_case::test_case("foo%bar", "fooXbar" => true)]
    #[test_case::test_case("foo%bar", "foobar" => true)]
    #[test_case::test_case("foo%bar", "fooba" => false)]
    #[test_case::test_case("%user_id%", "xuserXidz" => true)]
    #[test_case::test_case("%foo%", "afoob" => true)]
    #[test_case::test_case("foo", "FOO" => true ; "case-insensitive superset of LIKE")]
    #[test_case::test_case("foo", "food" => false ; "no wildcard = exact length")]
    #[test_case::test_case("a\\_c", "a_c" => true ; "escaped underscore is literal")]
    #[test_case::test_case("a\\_c", "abc" => false ; "escaped underscore does not match any char")]
    #[test_case::test_case("%", "" => true)]
    #[test_case::test_case("_", "" => false)]
    fn like_ci_is_a_superset_of_sql_like(pattern: &str, text: &str) -> bool {
        like_match_ci(pattern, text)
    }

    #[test_case::test_case("eq", "abc", "xxabcyy" => true ; "eq → containment superset")]
    #[test_case::test_case("like:tf_ngram3", "%a_b%", "zzaXbzz" => true)]
    #[test_case::test_case("like:tf_ngram3", "%a_b%", "zzabzz" => false)]
    fn deferred_row_eval_reproduces_the_original_predicate(kind: &str, value: &str, haystack: &str) -> bool {
        deferred_row_matches(kind, value, haystack)
    }

    #[test]
    fn regex_literal_substring_accepts_only_escaped_literals() {
        assert_eq!(regex_literal_substring("runServer"), Some("runServer".into()));
        assert_eq!(regex_literal_substring("svc\\.user-api"), Some("svc.user-api".into()));
        assert_eq!(regex_literal_substring("GET /v1/users"), Some("GET /v1/users".into()));
        for p in ["run.*", "^foo", "foo$", "a|b", "f(o)o", "a[bc]", "x{2}", "\\d+", "\\yword\\y", "\\w", "trailing\\", "", "a\\+b"] {
            assert_eq!(regex_literal_substring(p), None, "{p:?} must not decode to a literal");
        }
    }
}
