# Why `hashes` queries time out — the filter cannot reach the scan

**Reported 2026-09-03:** customer queries on the `hashes` column time out on
monoscope's issues pages. Diagnosed end to end; the mechanism is exact and the
fix has a real correctness precondition.

## The query

Issues pages and the endpoint dashboards both use array containment:

```sql
... AND hashes @> ARRAY['err:<hash>']        -- issue occurrences
... AND hashes @> ARRAY['{{var-endpointHash}}']  -- endpoint-stats.yaml, 7+ panels
```

DataFusion lowers `@>` to `array_has_all(hashes, [...])`.

## The mechanism, from a prod EXPLAIN

```
FilterExec: ... array_has_all(hashes@2, [x]) AND deleted IS DISTINCT FROM true
  DedupExec: keys=[timestamp, resource___service___name, id], mode=bounded/greatest
    SortPreservingMergeExec: [timestamp DESC]
      UnionExec
        OrderingProbeExec leg=mem   -> DataSourceExec
        OrderingProbeExec leg=delta -> GatedScanExec -> DeltaScanExec
```

**The predicate sits ABOVE `DedupExec`.** So the scan reads *every row in the time
window*, the dedup processes all of it, and only then is the tag matched. A
selective predicate becomes a full window scan — which is exactly a timeout on a
wide window.

## Why it cannot simply be pushed down — the schema says so

```yaml
- name: hashes
  data_type: "List(Utf8)"
  # The one column production UPDATEs on this table (monoscope's enrichment
  # appends tags). Columns are immutable by DEFAULT, because a filter on an
  # immutable column can be pushed below the merge-on-read dedup and pruned at
  # the scan; a filter on this one must stay above it, or an older version
  # could match a tag the winning version no longer carries.
  mutable: true
```

And it has **no `bloom_filter`** and **no `tantivy` entry** — the only column on
this table's hot path with neither. So there is no pruning and no prefilter
either: three mechanisms that could have narrowed the scan, and `hashes`
participates in none of them.

## The way out: the predicate is MONOTONE if tags are append-only

The stated hazard is "an older version could match a tag the winning version no
longer carries". That requires tag REMOVAL. The observed production statement only
appends:

```sql
update otel_logs_and_spans o
   set hashes = coalesce(o.hashes, array[]::text[]) || array[u.tag]
```

**If `hashes` is append-only, a newer version always holds a superset of an older
version's tags.** Then `array_has_all(hashes, [t])` is MONOTONE under
`version_append`: if any version matches, the winner matches. A monotone predicate
CAN be pushed below the dedup — it can only over-include rows, never drop a
winner — which is precisely the property the "must stay above" rule lacks in
general.

That unlocks all three missing mechanisms at once: scan-level filtering, a bloom
filter on the tag values, and a tantivy prefilter to select files.

**This is a CLIENT invariant, not one TimeFusion enforces**, so it must be
declared and defended, not assumed:

1. Add a schema flag (e.g. `monotone: true`, valid only with `mutable: true`)
   meaning "values are only ever added". Push-down is enabled by that flag alone,
   never inferred.
2. **Verify the invariant against monoscope's writer before enabling it** — one
   statement anywhere that removes a tag makes results silently wrong, and silent
   wrongness on an issues page is worse than a slow one.
3. Then index it: `tantivy: { indexed: true, tokenizer: raw }` on the list
   elements gives file-level prefiltering, and a bloom filter gives row-group
   pruning.

## Measure first, and the number to get

Before building any of it, get the selectivity: how many rows in a typical issues
window carry one `err:` tag. If it is a handful out of millions, prefiltering is
worth a great deal and the ordering above is right. If a tag matches a large
fraction, the scan is not the problem and the fix is the window or the projection
instead.

**Not built tonight.** The diagnosis is complete and measured; the fix turns on a
client invariant that must be confirmed first, and it touches merge-on-read
correctness — the one area where being wrong is invisible until someone trusts a
wrong answer.
