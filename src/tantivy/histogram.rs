//! Exact fixed-duration histograms over indexed documents and their visible row ordinals.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use anyhow::{Context, Result, ensure};
use tantivy::{
    DocId, Score, Searcher, SegmentReader, Term,
    collector::{Collector, FilterCollector, SegmentCollector},
    fastfield::Column,
    query::{BooleanQuery, Occur, Query, RangeQuery, TermQuery},
    schema::{FieldType, IndexRecordOption, Schema},
};

use super::{ROW_ORDINAL_FIELD, TS_FIELD};

/// Exact list membership. Binary operators cannot accidentally express an empty
/// conjunction, whose SQL semantics depend on null-array presence.
#[derive(Debug, Clone)]
pub enum Membership {
    Contains { column: String, value: String },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl Membership {
    pub fn columns(&self) -> BTreeSet<&str> {
        match self {
            Self::Contains { column, .. } => BTreeSet::from([column.as_str()]),
            Self::And(left, right) | Self::Or(left, right) => left.columns().into_iter().chain(right.columns()).collect(),
        }
    }
    /// Uses DataFusion's array membership operator for captured-row fallback.
    fn expression(&self, schema: &arrow::datatypes::Schema) -> Result<datafusion::logical_expr::Expr> {
        use arrow::datatypes::DataType;
        use datafusion::{
            common::{Column, ScalarValue},
            logical_expr::{Expr, lit},
        };
        Ok(match self {
            Self::Contains { column, value } => {
                let DataType::List(field) = schema.field_with_name(column)?.data_type() else {
                    anyhow::bail!("membership fallback requires a string list: {column}");
                };
                let value = match field.data_type() {
                    DataType::Utf8 => ScalarValue::Utf8(Some(value.clone())),
                    DataType::Utf8View => ScalarValue::Utf8View(Some(value.clone())),
                    _ => anyhow::bail!("membership fallback requires string elements: {column}"),
                };
                datafusion::functions_nested::expr_fn::array_has(Expr::Column(Column::from_name(column)), lit(value))
            }
            Self::And(left, right) => left.expression(schema)?.and(right.expression(schema)?),
            Self::Or(left, right) => left.expression(schema)?.or(right.expression(schema)?),
        })
    }

    /// Compiles literal terms only when the index manifest certifies element mode.
    /// No tokenizer or query parser may interpret punctuation in these values.
    pub fn query(&self, schema: &Schema, element_fields: &BTreeSet<String>) -> Result<Box<dyn Query>> {
        match self {
            Self::Contains { column, value } => {
                ensure!(element_fields.contains(column), "index lacks exact elements for {column}");
                let field = schema.get_field(column)?;
                let FieldType::Str(options) = schema.get_field_entry(field).field_type() else {
                    anyhow::bail!("element field must be text: {column}");
                };
                ensure!(options.get_indexing_options().is_some_and(|options| options.tokenizer() == "raw"), "element field must use raw terms: {column}");
                Ok(Box::new(TermQuery::new(Term::from_field_text(field, value), IndexRecordOption::Basic)))
            }
            Self::And(left, right) | Self::Or(left, right) => {
                let occur = if matches!(self, Self::And(..)) { Occur::Must } else { Occur::Should };
                Ok(Box::new(BooleanQuery::new(vec![(occur, left.query(schema, element_fields)?), (occur, right.query(schema, element_fields)?)])))
            }
        }
    }
}

/// A bounded histogram in timestamp microseconds. Empty buckets are omitted.
///
/// Bucket arithmetic uses integers, including for timestamps outside the exact
/// range of f64. Construction rejects unrepresentable bucket starts.
#[derive(Debug, Clone, Copy)]
pub struct HistogramWindow {
    start: i64,
    end: i64,
    width: i64,
    first_bucket: i64,
    bucket_count: usize,
}

impl HistogramWindow {
    pub fn bounds(&self) -> (i64, i64) {
        (self.start, self.end)
    }
    /// Counts captured physical rows through the same winner mask as the index.
    /// This performs no source reads and preserves SQL membership/null semantics.
    pub fn count_rows(
        &self, batches: &[arrow::record_batch::RecordBatch], visible: &arrow::buffer::BooleanBuffer, membership: Option<&Membership>,
    ) -> Result<BTreeMap<i64, u64>> {
        use arrow::{
            array::{Array, BooleanArray},
            datatypes::{DataType, TimeUnit},
        };
        use datafusion::{common::DFSchema, logical_expr::execution_props::ExecutionProps, physical_expr::create_physical_expr};
        let rows = batches.iter().try_fold(0_usize, |sum, batch| sum.checked_add(batch.num_rows())).context("histogram source row count overflow")?;
        ensure!(rows == visible.len(), "histogram fallback mask differs from physical source rows");
        let mut counts = BTreeMap::<i64, u64>::new();
        let mut offset = 0;
        for batch in batches {
            let schema = batch.schema();
            let predicate = membership
                .map(|predicate| -> Result<_> {
                    let expression = predicate.expression(&schema)?;
                    let physical = create_physical_expr(&expression, &DFSchema::try_from(schema.as_ref().clone())?, &ExecutionProps::new())?;
                    Ok(physical.evaluate(batch)?.into_array(batch.num_rows())?)
                })
                .transpose()?;
            let predicate =
                predicate.as_ref().map(|array| array.as_any().downcast_ref::<BooleanArray>().context("membership result is not Boolean")).transpose()?;
            let timestamps = batch.column_by_name("timestamp").context("histogram source is missing timestamp")?;
            ensure!(
                matches!(timestamps.data_type(), DataType::Int64 | DataType::Timestamp(TimeUnit::Microsecond, _)),
                "histogram timestamp must use microseconds"
            );
            let values = crate::read::bound_slice(timestamps).context("invalid histogram timestamp representation")?;
            for (row, &timestamp) in values.iter().enumerate() {
                if !visible.value(offset + row)
                    || timestamps.is_null(row)
                    || !(self.start..self.end).contains(&timestamp)
                    || predicate.is_some_and(|predicate| predicate.is_null(row) || !predicate.value(row))
                {
                    continue;
                }
                let bucket =
                    i128::from(self.first_bucket) + (i128::from(timestamp) - i128::from(self.first_bucket)) / i128::from(self.width) * i128::from(self.width);
                let count = counts.entry(i64::try_from(bucket)?).or_default();
                *count = count.checked_add(1).context("histogram count overflow")?;
            }
            offset += batch.num_rows();
        }
        Ok(counts)
    }

    /// Validates a half-open time window and its maximum number of buckets.
    pub fn new(start: i64, end: i64, width: i64, origin: i64, max_buckets: usize) -> Result<Self> {
        ensure!(start < end, "histogram window must be nonempty");
        ensure!(width > 0, "histogram width must be positive");
        let bucket = |timestamp: i64| {
            let origin = i128::from(origin);
            let width = i128::from(width);
            origin + (i128::from(timestamp) - origin).div_euclid(width) * width
        };
        let first_bucket = i64::try_from(bucket(start))?;
        let count = (bucket(end - 1) - i128::from(first_bucket)) / i128::from(width) + 1;
        let bucket_count = usize::try_from(count)?;
        ensure!(bucket_count <= max_buckets, "histogram bucket limit exceeded");
        Ok(Self { start, end, width, first_bucket, bucket_count })
    }

    /// Counts matches after applying visibility from the caller's source snapshot.
    ///
    /// The caller must verify index coverage, exact field representation, and
    /// physical ordinal validity against that snapshot. `visible` must exclude
    /// superseded versions and tombstones across all storage legs, including memory.
    /// Tantivy's own live-document bitmap alone does not establish this contract.
    pub fn search<P>(&self, searcher: &Searcher, predicate: Box<dyn Query>, visible: P) -> Result<BTreeMap<i64, u64>>
    where
        P: Fn(u64) -> bool + Clone + Send + Sync + 'static,
    {
        // Fail on legacy indexes without physical ordinals, rather than silently
        // treating a missing mask column as an empty result.
        for segment in searcher.segment_readers() {
            segment.fast_fields().u64(ROW_ORDINAL_FIELD)?;
        }
        let time = RangeQuery::new_i64_bounds(TS_FIELD.into(), Bound::Included(self.start), Bound::Excluded(self.end));
        let query = BooleanQuery::new(vec![(Occur::Must, predicate), (Occur::Must, Box::new(time))]);
        let collector = FilterCollector::new(ROW_ORDINAL_FIELD.into(), visible, HistogramCollector(*self));
        Ok(searcher.search(&query, &collector)?)
    }
}

struct HistogramCollector(HistogramWindow);

impl Collector for HistogramCollector {
    type Fruit = BTreeMap<i64, u64>;
    type Child = HistogramSegment;

    fn for_segment(&self, _: u32, reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(HistogramSegment { timestamps: reader.fast_fields().i64(TS_FIELD)?, window: self.0, counts: vec![0; self.0.bucket_count] })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Vec<u64>>) -> tantivy::Result<Self::Fruit> {
        let mut counts = vec![0_u64; self.0.bucket_count];
        for fruit in fruits {
            for (total, count) in counts.iter_mut().zip(fruit) {
                *total = total.checked_add(count).ok_or_else(|| tantivy::TantivyError::InvalidArgument("histogram count overflow".into()))?;
            }
        }
        Ok(counts
            .into_iter()
            .enumerate()
            .filter(|(_, count)| *count != 0)
            .map(|(i, count)| {
                // Construction proves every bucket lies between first_bucket and end.
                let key = i128::from(self.0.first_bucket) + i as i128 * i128::from(self.0.width);
                (key as i64, count)
            })
            .collect())
    }
}

struct HistogramSegment {
    timestamps: Column<i64>,
    window: HistogramWindow,
    counts: Vec<u64>,
}

impl SegmentCollector for HistogramSegment {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: DocId, _: Score) {
        if let Some(timestamp) = self.timestamps.first(doc)
            && timestamp >= self.window.start
            && timestamp < self.window.end
        {
            let bucket = (i128::from(timestamp) - i128::from(self.window.first_bucket)) / i128::from(self.window.width);
            // A segment has at most u32::MAX documents; its counts cannot overflow u64.
            self.counts[bucket as usize] += 1;
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::{
        Index, doc,
        schema::{FAST, INDEXED, STRING, Schema},
    };

    #[test]
    fn histogram_matches_visible_rows_across_segments_and_boundaries() -> Result<()> {
        for base in [0_i64, 1_788_941_234_567_890, 9_007_199_254_740_992, i64::MAX - 100] {
            let mut schema = Schema::builder();
            let timestamp = schema.add_i64_field(TS_FIELD, FAST | INDEXED);
            let ordinal = schema.add_u64_field(ROW_ORDINAL_FIELD, FAST);
            let hashes = schema.add_text_field("hashes", STRING);
            let index = Index::create_in_ram(schema.build());
            let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
            writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
            let rows = [(-11, true), (-10, true), (-1, false), (0, true), (9, true), (10, true), (19, true), (20, true)];
            for (i, (offset, _)) in rows.iter().enumerate() {
                writer.add_document(doc!(timestamp => base + offset, ordinal => i as u64, hashes => "a", hashes => "a", hashes => "b"))?;
                if i == 3 {
                    writer.commit()?;
                }
            }
            writer.commit()?;
            let reader = index.reader()?;
            for width in [1, 7, 10, 17] {
                for origin in [base, base - 3] {
                    let window = HistogramWindow::new(base - 10, base + 20, width, origin, 100)?;
                    let term = |value: &str| Box::new(Membership::Contains { column: "hashes".into(), value: value.into() });
                    let union = Membership::Or(term("a"), term("b"));
                    assert!(union.query(&index.schema(), &BTreeSet::new()).is_err());
                    let query = union.query(&index.schema(), &BTreeSet::from(["hashes".into()]))?;
                    let actual = window.search(&reader.searcher(), query, move |i| rows[i as usize].1)?;
                    // Enumerate bucket intervals independently of the collector's division.
                    let expected = (-40..40)
                        .map(|k| i128::from(origin) + k * i128::from(width))
                        .filter_map(|lo| {
                            let count = rows
                                .iter()
                                .filter(|(offset, visible)| {
                                    let t = i128::from(base) + i128::from(*offset);
                                    *visible && (-10..20).contains(offset) && t >= lo && t < lo + i128::from(width)
                                })
                                .count() as u64;
                            (count > 0).then_some((lo as i64, count))
                        })
                        .collect::<BTreeMap<_, _>>();
                    assert_eq!(actual, expected, "base={base}, width={width}, origin={origin}");
                }
            }
        }
        for (start, end, width, origin, limit) in [(0, 0, 1, 0, 10), (0, 1, 0, 0, 10), (0, 10, 1, 0, 9), (i64::MIN, 0, 3, 0, 10)] {
            assert!(HistogramWindow::new(start, end, width, origin, limit).is_err());
        }
        Ok(())
    }
}
