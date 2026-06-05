//! Shared accessors that read uniformly across the multiple Arrow encodings a
//! single logical type can take. delta-rs/Parquet may hand back `Utf8`,
//! `LargeUtf8`, or `Utf8View` for the same column depending on
//! `schema_force_view_types`, so call sites that would otherwise need a
//! downcast-cascade (and a duplicated per-encoding loop body) use these instead.
//!
//! This generalizes the `BinaryAccessor` pattern already used for Variant
//! decoding in `functions.rs`.

use datafusion::arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use datafusion::error::{DataFusionError, Result as DFResult};

/// Reads `&str` values from any of the three Arrow string encodings without the
/// caller having to branch on the concrete array type.
pub enum StrAccessor<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> StrAccessor<'a> {
    /// Borrow `col` as a string accessor, or error if it is not a string array.
    pub fn try_new(col: &'a ArrayRef) -> DFResult<Self> {
        if let Some(a) = col.as_any().downcast_ref::<StringViewArray>() {
            Ok(Self::View(a))
        } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(a))
        } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
            Ok(Self::LargeUtf8(a))
        } else {
            Err(DataFusionError::Execution(format!(
                "expected a string array (Utf8/LargeUtf8/Utf8View), got {:?}",
                col.data_type()
            )))
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Utf8(a) => a.len(),
            Self::LargeUtf8(a) => a.len(),
            Self::View(a) => a.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        match self {
            Self::Utf8(a) => a.is_null(i),
            Self::LargeUtf8(a) => a.is_null(i),
            Self::View(a) => a.is_null(i),
        }
    }

    #[inline]
    pub fn value(&self, i: usize) -> &str {
        match self {
            Self::Utf8(a) => a.value(i),
            Self::LargeUtf8(a) => a.value(i),
            Self::View(a) => a.value(i),
        }
    }

    /// Null-aware read: `None` if row `i` is null, else the string value.
    #[inline]
    pub fn get(&self, i: usize) -> Option<&str> {
        if self.is_null(i) { None } else { Some(self.value(i)) }
    }
}
