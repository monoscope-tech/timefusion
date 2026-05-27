# variant_functions.slt disabled pending variant_get → text coercion

Many `->>'key'` cases in this file expect Postgres-style text coercion
(e.g. integer 10 → "10", boolean true → "true"), but the underlying
`parquet_variant_compute::variant_get(..., "Utf8")` returns NULL for
non-string Variant leaves. The string cases were corrected (`->>` on
text returns unquoted text per Postgres semantics) but the numeric/
boolean/array cases need a coercion shim before this file can be
re-enabled. Rename back to `.slt` once `variant_get` returns the
text representation of the leaf value.
