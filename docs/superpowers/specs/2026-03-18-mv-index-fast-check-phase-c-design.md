# MV Index Fast Admin Check — Phase C: Unify to BIT_XOR

## Goal

Replace the MV index checksum from `SUM(handle_crc * element_crc)` to `BIT_XOR(per_entry_crc)`, unifying the aggregation with normal indexes. This eliminates `JSON_SUM_CRC32`, `JSONCRC32Mod`, and fixes a latent type-safety bug.

## Background

After Phase A, the `indexCheckBuilder` interface cleanly separates MV and normal index logic. The two builders currently use different aggregation:
- **normalCheckBuilder**: `BIT_XOR(CRC32(MD5(CONCAT_WS(handle, idx_cols))))`
- **mvCheckBuilder**: `SUM(handle_crc * JSON_SUM_CRC32(array))` with `MOD 1024`

Phase C unifies both to BIT_XOR and fixes a type-safety issue in the process.

## Math

For a table row with handle `h`, non-array index cols `n`, and MV index array `[a, b]` (after dedup):

**Index side** (each MV entry is independent):
```
BIT_XOR(CRC32(MD5(CONCAT_WS(0x2, h, n, a))), CRC32(MD5(CONCAT_WS(0x2, h, n, b))))
```

**Table side** (per-handle XOR, then cross-handle BIT_XOR):
```
BIT_XOR( CRC32(h||n||a) ⊕ CRC32(h||n||b) )
```

These are equal because XOR is associative: `(A ⊕ B) ⊕ C = A ⊕ B ⊕ C`.

**Why non-array cols must be in the CRC**: Without `n`, corruption in non-array columns goes undetected:
```
Index mvi(a, CAST(j AS UNSIGNED ARRAY)), row: handle=1, a=10, j=[1,2]
If a is corrupted to 99 in the index:
  Without n: CRC32("1,1") ⊕ CRC32("1,2") — same on both sides → missed ✗
  With n:    CRC32("1,99,1") vs CRC32("1,10,1") — different → detected ✓
```

## New Builtin Function: `JSON_ARRAY_XOR_CRC32`

**Signature**: `JSON_ARRAY_XOR_CRC32(array_expr, prefix_string)`

The prefix is pre-computed in SQL:
```sql
JSON_ARRAY_XOR_CRC32(array_expr, CONCAT_WS(0x2, handle_cols, non_array_cols))
```

**Semantics**:
1. Evaluate `array_expr` as a JSON array
2. Deduplicate elements (same as `JSON_SUM_CRC32`)
3. For each unique element `e`:
   a. Cast `e` to the target type (e.g., `CAST(e AS DOUBLE)`) — producing a typed Datum
   b. Convert to string via `Datum.EvalString()` — the **same path** that `CONCAT_WS` uses on the index side
   c. Compute `CRC32(MD5(CONCAT_WS(0x2, prefix_string, str)))`
4. XOR all per-element CRC32s together
5. Return the result as UNSIGNED INT

### Type Safety: Why EvalString Matters

The checksum is correct only when the table-side and index-side string representations of array elements are **identical**. The current `JSON_SUM_CRC32` uses a custom `convertJSON2String` function with per-type conversion logic that is a **different code path** from `EvalString` used by `CRC32()`/`CONCAT_WS()` on the index side:

| Type | `convertJSON2String` (current) | `EvalString` (index side) | Risk |
|------|-------------------------------|--------------------------|------|
| ETInt | `strconv.Itoa(int(val))` | standard int-to-string | Low |
| ETReal | `strconv.FormatFloat(val, 'f', -1, digit)` | may use different format | **High** |
| ETDecimal | not supported | n/a | Blocked |
| ETDatetime | `res.String()` | datetime-to-string | Medium |
| ETDuration | `dur.String()` | duration-to-string | Medium |

`JSON_ARRAY_XOR_CRC32` fixes this by using `EvalString` for all types:

```go
// Pseudocode for element processing:
for each element in json_array {
    datum := castJSONElementToTargetType(element, targetFieldType)
    str := datum.EvalString(ctx)  // Same code path as CONCAT_WS on index side
    crc := crc32.ChecksumIEEE([]byte(CONCAT_WS(0x2, prefix, str)))
    result ^= crc
}
```

This means **all CAST ARRAY types become safe**, including ETDecimal and ETReal. The `indexSupportFastCheck` type restriction in builder.go can be removed.

## SQL Changes

### Checksum Phase

**Table side**:
```sql
SELECT BIT_XOR(JSON_ARRAY_XOR_CRC32(array_expr, CONCAT_WS(0x2, handle, non_array))),
       bucket, COUNT(*)
FROM t USE INDEX()
WHERE json_length_filter AND whereKey = 0
GROUP BY bucket
```

**Index side**:
```sql
SELECT BIT_XOR(CRC32(MD5(CONCAT_WS(0x2, handle, non_array, hidden_col)))),
       bucket, COUNT(*)
FROM t USE INDEX(idx)
WHERE whereKey = 0
GROUP BY bucket
```

### Detail (Check Row) Phase

**Table side**: `JSON_ARRAY_XOR_CRC32` per row as the checksum column, ordered by handle.

**Index side**: Subquery with `GROUP BY handle`, using `BIT_XOR(CRC32(MD5(CONCAT_WS(...))))` per handle as the checksum, with `JSON_ARRAYAGG` for array values.

## Implementation: `mvXorCheckBuilder`

New builder implementing `indexCheckBuilder`, replacing `mvCheckBuilder`:

- `handleChecksum()`: `CRC32(MD5(CONCAT_WS(handle_cols)))` — same as `normalCheckBuilder` (non-array cols are now in the per-entry CRC, not in the bucketing expression)
- `buildChecksumQuery()`: table side uses `BIT_XOR(JSON_ARRAY_XOR_CRC32(...))`, index side uses `BIT_XOR(CRC32(MD5(CONCAT_WS(...))))`
- `buildCheckRowQuery()`: table side uses `JSON_ARRAY_XOR_CRC32` per row; index side uses subquery + GROUP BY handle
- `getRecords()`: same as current `mvCheckBuilder`

## Migration

1. Add `JSON_ARRAY_XOR_CRC32` in parser/planner/expression (same restricted-context approach as `JSON_SUM_CRC32`)
2. Add `mvXorCheckBuilder` implementing `indexCheckBuilder`
3. Swap `newIndexCheckBuilder` to use `mvXorCheckBuilder`
4. Remove type restriction in `indexSupportFastCheck`
5. Delete `mvCheckBuilder`, `JSON_SUM_CRC32`, `JSONCRC32Mod`, and `convertJSON2String`

## Verification

Each supported type needs an explicit test: insert data → `admin check table` (fast) → must pass. Edge cases:

- Float: `[1.0, 2.5, 0.1]` (formatting precision)
- Decimal: `[1.10, 2.00]` (trailing zeros)
- Datetime: `['2024-01-01 00:00:00']` (with/without time)
- Duration: `['12:30:00']` (with/without fractional seconds)
- Empty array: `[]` (no index entries)
- NULL JSON (creates one NULL index entry)
- Duplicates: `[1, 1, 2]` (dedup must match index behavior)

## Benefits

- Unified aggregation: both MV and normal indexes use `BIT_XOR`
- No `MOD 1024` overflow protection (XOR is bounded to 32 bits)
- Type-safe: `EvalString`-based conversion supports all CAST ARRAY types
- `handleChecksum()` unified between builders
- Correctness fix: eliminates `convertJSON2String` divergence risk

## Trade-offs

- Still needs one internal-only builtin function (replacement, not addition)
- Still can't be pushed to TiKV (same limitation as `JSON_SUM_CRC32`)
