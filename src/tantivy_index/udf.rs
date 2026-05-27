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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BooleanBuilder, StringArray, StringViewArray},
    datatypes::DataType,
};
use datafusion::{
    common::Result as DFResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
};

pub const TEXT_MATCH_NAME: &str = "text_match";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TextMatchUdf {
    sig: Signature,
}

impl Default for TextMatchUdf {
    fn default() -> Self {
        Self {
            sig: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TextMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
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
        for i in 0..n {
            match (col_str(i), pat_str(i)) {
                (Some(haystack), Some(needle)) => {
                    // Needles arriving from the LIKE rewriter carry tantivy
                    // syntax (`'foo*'` for prefix, `'foo'` for substring on
                    // ngram3). For row-level substring matching we strip the
                    // wildcards so 'batch*' substring-matches 'batch_test'.
                    let h_low = haystack.to_lowercase();
                    let ok = needle
                        .to_lowercase()
                        .split_whitespace()
                        .map(|tok| tok.trim_matches(|c: char| c == '*' || c == '?'))
                        .all(|tok| !tok.is_empty() && h_low.contains(tok));
                    b.append_value(ok);
                }
                _ => b.append_value(false),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
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
        // Variant or anything else — degrade to never-match (tantivy still works
        // on the indexed side; mem-buffer post-filter would need json eval here).
        _ => Box::new(|_| None),
    }
}

pub fn text_match_udf() -> ScalarUDF {
    ScalarUDF::from(TextMatchUdf::default())
}

/// Detect a `text_match(col, 'q')` predicate and extract its column name and
/// query string. Returns `Some` only if the call shape is exactly that.
pub fn extract_text_match(expr: &datafusion::logical_expr::Expr) -> Option<TextMatchPred> {
    use datafusion::{logical_expr::Expr, scalar::ScalarValue};
    let Expr::ScalarFunction(sf) = expr else { return None };
    if sf.func.name() != TEXT_MATCH_NAME {
        return None;
    }
    if sf.args.len() != 2 {
        return None;
    }
    let col = match &sf.args[0] {
        Expr::Column(c) => c.name.clone(),
        _ => return None,
    };
    let q = match &sf.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
        _ => return None,
    };
    Some(TextMatchPred { column: col, query: q })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextMatchPred {
    pub column: String,
    pub query:  String,
}

/// Walk filter expressions, pulling out all `text_match` calls.
pub fn collect_text_matches(filters: &[datafusion::logical_expr::Expr]) -> Vec<TextMatchPred> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut out = Vec::new();
    for f in filters {
        let _ = f.apply(|e| {
            if let Some(p) = extract_text_match(e) {
                out.push(p);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    out
}
