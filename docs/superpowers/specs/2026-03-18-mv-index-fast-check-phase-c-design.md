# MV Index Fast Admin Check — Phase C: Unify to BIT_XOR

## Goal

Replace the MV index checksum approach from `SUM(handle_crc * element_crc)` to `BIT_XOR(per_entry_crc)`, unifying the aggregation function with normal indexes. This eliminates `JSON_SUM_CRC32` and the `JSONCRC32Mod` overflow protection.

## Background

After Phase A, we have a clean `indexCheckBuilder` interface. The two builders use different aggregation:
- **normalCheckBuilder**: `BIT_XOR(CRC32(MD5(CONCAT_WS(handle, idx_cols))))`
- **mvCheckBuilder**: `SUM(handle_crc * JSON_SUM_CRC32(array))` with `MOD 1024`

Phase C unifies both to BIT_XOR.

## Math

For a table row with handle `h` and MV index array `[a, b, c]` (after dedup):
- Per-entry checksums: `CRC32(MD5(CONCAT_WS(h, a)))`, `CRC32(MD5(CONCAT_WS(h, b)))`, `CRC32(MD5(CONCAT_WS(h, c)))`
- Per-handle XOR: `CRC32(h||a) ⊕ CRC32(h||b) ⊕ CRC32(h||c)`

**Index side**: BIT_XOR over all entries directly — each entry contributes independently.

**Table side**: BIT_XOR over per-handle XORs. Because XOR is associative:
`(CRC32(h1||a) ⊕ CRC32(h1||b)) ⊕ CRC32(h2||c) = CRC32(h1||a) ⊕ CRC32(h1||b) ⊕ CRC32(h2||c)`

Both sides produce the same result.

## New Builtin Function: `JSON_ARRAY_XOR_CRC32`

Replaces `JSON_SUM_CRC32`. Takes 2 parameters: the array expression and a pre-computed prefix string.

**Signature**: `JSON_ARRAY_XOR_CRC32(array_expr, prefix_string)`

The prefix is computed in SQL before being passed to the function:
```sql
JSON_ARRAY_XOR_CRC32(array_expr, CONCAT_WS(0x2, handle_cols, non_array_cols))
```

**Semantics**:
1. Evaluate the JSON array from `array_expr`
2. Deduplicate elements (same as `JSON_SUM_CRC32`)
3. For each unique element `e`: compute `CRC32(MD5(CONCAT_WS(0x2, prefix_string, e)))`
4. XOR all per-element CRC32s together
5. Return the result as UNSIGNED INT

**Why prefix must include non-array cols**: Without them, corruption in non-array index columns would produce identical checksums on both sides and go undetected. For example, if MV index `(a, CAST(j AS UNSIGNED ARRAY))` has `a` corrupted from 10 to 99, only including `a` in the per-element CRC will catch it.

**Why not reuse JSON_SUM_CRC32**: The current function only takes the array — it doesn't include handle/non-array columns in the per-element CRC. For BIT_XOR to work, these must be baked into each element's CRC (since we can't use multiplication to separate them, unlike the SUM approach).

## SQL Changes

### Checksum Phase

**Table side** (current):
```sql
SELECT CAST(SUM((handle_crc MOD 1024) * JSON_SUM_CRC32(array_expr)) AS SIGNED), bucket, COUNT(*)
FROM t USE INDEX() WHERE filter AND whereKey = 0 GROUP BY bucket
```

**Table side** (Phase C):
```sql
SELECT BIT_XOR(JSON_ARRAY_XOR_CRC32(array_expr, CONCAT_WS(0x2, handle_cols, non_array_cols))), bucket, COUNT(*)
FROM t USE INDEX() WHERE filter AND whereKey = 0 GROUP BY bucket
```

**Index side** (current):
```sql
SELECT CAST(SUM((handle_crc MOD 1024) * (CRC32(hidden_col) MOD 1024)) AS SIGNED), bucket, COUNT(*)
FROM t USE INDEX(idx) WHERE whereKey = 0 GROUP BY bucket
```

**Index side** (Phase C):
```sql
SELECT BIT_XOR(CRC32(MD5(CONCAT_WS(0x2, handle_cols, non_array_cols, hidden_col)))), bucket, COUNT(*)
FROM t USE INDEX(idx) WHERE whereKey = 0 GROUP BY bucket
```

### Detail (Check Row) Phase

**Table side**: Similar change — use `JSON_ARRAY_XOR_CRC32` as the per-row checksum.

**Index side**: GROUP BY handle, compute `BIT_XOR(CRC32(MD5(CONCAT_WS(...))))` per handle, then SELECT with individual rows.

## Implementation: `mvXorCheckBuilder`

A new builder implementing `indexCheckBuilder`, replacing `mvCheckBuilder`:

```go
type mvXorCheckBuilder struct {
    tblName    string
    handleCols []string
    pkTypes    []*types.FieldType
    idxInfo    *model.IndexInfo
    tblInfo    *model.TableInfo
}
```

- `handleChecksum()`: Same as `normalCheckBuilder` — `CRC32(MD5(CONCAT_WS(handle_cols)))` (no non-array cols needed here since they're in the per-entry CRC now).
- `buildChecksumQuery()`: Uses `BIT_XOR(JSON_ARRAY_XOR_CRC32(array, CONCAT_WS(0x2, handle, non_array)))` on table side, `BIT_XOR(CRC32(MD5(CONCAT_WS(0x2, handle, non_array, element))))` on index side.
- `buildCheckRowQuery()`: Table side uses `JSON_ARRAY_XOR_CRC32` per row; index side GROUPs BY handle with `BIT_XOR`.
- `getRecords()`: Same as `mvCheckBuilder`.

## Migration

1. Add `JSON_ARRAY_XOR_CRC32` builtin in parser/planner/expression (same restricted-context approach as `JSON_SUM_CRC32`)
2. Add `mvXorCheckBuilder` implementing `indexCheckBuilder`
3. Swap `newIndexCheckBuilder` to use `mvXorCheckBuilder` for MV indexes
4. Delete `mvCheckBuilder`, `JSON_SUM_CRC32`, and `JSONCRC32Mod`

## Benefits

- MV and normal indexes use the same outer aggregation (`BIT_XOR`)
- No `MOD 1024` overflow protection needed (XOR is bounded to 32 bits)
- Simpler checksum math
- `handleChecksum()` could potentially be unified between builders

## Trade-offs

- New function (`JSON_ARRAY_XOR_CRC32`) has a different interface than `JSON_SUM_CRC32` (2 params: array + prefix string), but the function itself is simpler (XOR instead of modular SUM)
- Still can't be pushed to TiKV (same limitation)
- Parser/expression layer still has one internal-only function (but it's a replacement, not an addition)
