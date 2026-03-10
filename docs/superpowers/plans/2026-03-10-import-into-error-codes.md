# IMPORT INTO: Granular Error Codes Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace catch-all and overloaded IMPORT INTO error codes with 22 new granular error codes, each mapping 1:1 to a distinct failure mode.

**Architecture:** Add error code constants, message templates, and error variables in the errno/exeerrors layer, then update call sites in precheck, import, chunk_process, table_import, and lightning parsers. Wrap execution-phase errors at the IMPORT INTO boundary rather than deep inside Lightning internals. For CSV/SQL syntax errors, detect error patterns in `chunk_process.go` since both CSV and SQL parsers produce `"syntax error: ..."` messages.

**Tech Stack:** Go, TiDB error framework (`dbterror`, `errno`, `exeerrors`), Lightning mydump parsers.

**Spec:** `docs/superpowers/specs/2026-03-10-import-into-error-codes-design.md`

**Spec deviations:**
- `ErrLoadDataChecksumMismatch` (8401): Simplified to 1 arg (table name) vs spec's 7 args. Detailed checksum values are logged separately.
- `ErrLoadDataUnknownColumns` (8400): Defined but has no call site — `common.ErrUnknownColumns` has zero usages in the current codebase. The error code is reserved for future use when header column validation is added.
- CSV/SQL syntax errors: Detected at the IMPORT INTO boundary in `chunk_process.go` via error message pattern matching, rather than wrapping inside Lightning internals. This keeps Lightning code shared with BR unchanged.

---

## Chunk 1: Error Code Definitions (errno + exeerrors)

### Task 1: Add error code constants to errcode.go

**Files:**
- Modify: `pkg/errno/errcode.go:1085` (after `ErrLoadDataPreCheckFailed`)

- [ ] **Step 1: Add 22 new error code constants**

After the existing `ErrLoadDataPreCheckFailed = 8173` line (line 1085), add the new constants. Do NOT remove `ErrLoadDataPreCheckFailed` yet — we remove it in a later task after all call sites are updated.

```go
	// IMPORT INTO granular error codes: pre-check phase
	ErrLoadDataActiveJobExists         = 8182
	ErrLoadDataNoFilesMatched          = 8183
	ErrLoadDataTargetTableNotEmpty     = 8184
	ErrLoadDataPiTRRunning             = 8185
	ErrLoadDataCDCRunning              = 8186
	ErrLoadDataCloudStorageUnsupported = 8187
	ErrLoadDataCloudStorageAccessDenied = 8188

	// IMPORT INTO granular error codes: file access phase
	ErrLoadDataInvalidFilePath    = 8189
	ErrLoadDataGlobPatternInvalid = 8190
	ErrLoadDataFileOpenFailed     = 8191
	ErrLoadDataDirWalkFailed      = 8192

	// IMPORT INTO granular error codes: CSV/SQL parsing phase
	ErrLoadDataCSVSyntaxError      = 8193
	ErrLoadDataSQLSyntaxError      = 8194
	ErrLoadDataRowSizeTooLarge     = 8195
	ErrLoadDataColumnCountMismatch = 8196

	// IMPORT INTO granular error codes: parquet parsing phase
	ErrLoadDataParquetUnsupportedType = 8197
	ErrLoadDataParquetReadFailed      = 8198
	ErrLoadDataParquetDecimalError    = 8199

	// IMPORT INTO granular error codes: post-processing / execution
	ErrLoadDataUnknownColumns        = 8400
	ErrLoadDataChecksumMismatch      = 8401
	ErrLoadDataEncodeKVFailed        = 8402
	ErrLoadDataParquetCompressedFile = 8403
```

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/errno/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pkg/errno/errcode.go
git commit -m "errno: add 22 new IMPORT INTO error code constants"
```

### Task 2: Add error message templates to errname.go

**Files:**
- Modify: `pkg/errno/errname.go` (after the `ErrLoadDataPreCheckFailed` entry, around line 1079)

- [ ] **Step 1: Add 22 new error message entries**

After the existing `ErrLoadDataPreCheckFailed` entry, add the new message templates. Follow the existing pattern using `mysql.Message(format, argPositions)`:

```go
	ErrLoadDataActiveJobExists:         mysql.Message("An active IMPORT INTO job on table '%s.%s' already exists. Cancel or wait for the existing job before starting a new one.", nil),
	ErrLoadDataNoFilesMatched:          mysql.Message("No data files found matching path '%s'. Verify the path, glob pattern, and that files are not empty.", nil),
	ErrLoadDataTargetTableNotEmpty:     mysql.Message("Target table '%s.%s' is not empty. Truncate the table or set a different ON DUPLICATE KEY strategy.", nil),
	ErrLoadDataPiTRRunning:             mysql.Message("IMPORT INTO is not supported while PiTR log streaming tasks are active: %s. Pause or remove PiTR tasks first.", nil),
	ErrLoadDataCDCRunning:              mysql.Message("IMPORT INTO is not supported while CDC changefeeds are active. %s", nil),
	ErrLoadDataCloudStorageUnsupported: mysql.Message("Unsupported cloud storage URI scheme '%s'. Only 's3' and 'gcs' are supported.", nil),
	ErrLoadDataCloudStorageAccessDenied: mysql.Message("Cloud storage access denied at '%s': %s. Verify credentials and bucket permissions.", nil),
	ErrLoadDataInvalidFilePath:         mysql.Message("Invalid file path '%s': %s.", nil),
	ErrLoadDataGlobPatternInvalid:      mysql.Message("Invalid glob pattern in path '%s': %s.", nil),
	ErrLoadDataFileOpenFailed:          mysql.Message("Cannot open data file '%s': %s. Verify the file exists and is accessible.", nil),
	ErrLoadDataDirWalkFailed:           mysql.Message("Failed to scan directory for data files: %s. Verify the path and permissions.", nil),
	ErrLoadDataCSVSyntaxError:          mysql.Message("CSV syntax error in file '%s' at offset %d: %s. Check delimiters, quoting, and escape characters.", nil),
	ErrLoadDataSQLSyntaxError:          mysql.Message("SQL syntax error in file '%s' at offset %d: %s. Verify the SQL dump file contains valid INSERT statements.", nil),
	ErrLoadDataRowSizeTooLarge:         mysql.Message("Row size exceeds the maximum allowed value of txn-entry-size-limit in file '%s' at offset %d. Split large rows or increase txn-entry-size-limit.", nil),
	ErrLoadDataColumnCountMismatch:     mysql.Message("Column count mismatch in file '%s': expected %d columns but got %d. Verify the file format matches the table schema.", nil),
	ErrLoadDataParquetUnsupportedType:  mysql.Message("Unsupported parquet data type '%s' in file '%s'. Supported types: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY.", nil),
	ErrLoadDataParquetReadFailed:       mysql.Message("Failed to read parquet file '%s': %s. Verify the file is a valid, uncorrupted parquet file.", nil),
	ErrLoadDataParquetDecimalError:     mysql.Message("Invalid decimal value in parquet file '%s': %s. Verify decimal precision and scale match the target column.", nil),
	ErrLoadDataUnknownColumns:          mysql.Message("Unknown columns in CSV header (%s) for table '%s'. Verify the header row matches the target table column names.", nil),
	ErrLoadDataChecksumMismatch:        mysql.Message("Checksum mismatch after importing table '%s'. Data may be corrupted; re-import the table.", nil),
	ErrLoadDataEncodeKVFailed:          mysql.Message("Failed to encode row to KV pair in file '%s' at offset %d: %s. Check that data values match the column types.", nil),
	ErrLoadDataParquetCompressedFile:   mysql.Message("Whole-file compression is not supported for parquet files: '%s'. Use parquet column-level compression instead.", nil),
```

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/errno/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pkg/errno/errname.go
git commit -m "errno: add 22 new IMPORT INTO error message templates"
```

### Task 3: Add error variables to exeerrors/errors.go

**Files:**
- Modify: `pkg/util/dbterror/exeerrors/errors.go:105` (after `ErrLoadDataPreCheckFailed`)

- [ ] **Step 1: Add 22 new error variable declarations**

After the existing `ErrLoadDataPreCheckFailed` line (line 105), add:

```go
	// IMPORT INTO granular errors: pre-check phase
	ErrLoadDataActiveJobExists          = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataActiveJobExists)
	ErrLoadDataNoFilesMatched           = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataNoFilesMatched)
	ErrLoadDataTargetTableNotEmpty      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataTargetTableNotEmpty)
	ErrLoadDataPiTRRunning              = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataPiTRRunning)
	ErrLoadDataCDCRunning               = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataCDCRunning)
	ErrLoadDataCloudStorageUnsupported  = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataCloudStorageUnsupported)
	ErrLoadDataCloudStorageAccessDenied = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataCloudStorageAccessDenied)

	// IMPORT INTO granular errors: file access phase
	ErrLoadDataInvalidFilePath    = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataInvalidFilePath)
	ErrLoadDataGlobPatternInvalid = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataGlobPatternInvalid)
	ErrLoadDataFileOpenFailed     = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataFileOpenFailed)
	ErrLoadDataDirWalkFailed      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataDirWalkFailed)

	// IMPORT INTO granular errors: CSV/SQL parsing phase
	ErrLoadDataCSVSyntaxError      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataCSVSyntaxError)
	ErrLoadDataSQLSyntaxError      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataSQLSyntaxError)
	ErrLoadDataRowSizeTooLarge     = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataRowSizeTooLarge)
	ErrLoadDataColumnCountMismatch = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataColumnCountMismatch)

	// IMPORT INTO granular errors: parquet parsing phase
	ErrLoadDataParquetUnsupportedType = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataParquetUnsupportedType)
	ErrLoadDataParquetReadFailed      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataParquetReadFailed)
	ErrLoadDataParquetDecimalError    = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataParquetDecimalError)

	// IMPORT INTO granular errors: post-processing / execution
	ErrLoadDataUnknownColumns        = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataUnknownColumns)
	ErrLoadDataChecksumMismatch      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataChecksumMismatch)
	ErrLoadDataEncodeKVFailed        = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataEncodeKVFailed)
	ErrLoadDataParquetCompressedFile = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataParquetCompressedFile)
```

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/util/dbterror/exeerrors/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pkg/util/dbterror/exeerrors/errors.go
git commit -m "exeerrors: add 22 new IMPORT INTO error variables"
```

## Chunk 2: Pre-check Phase Call Site Updates

### Task 4: Update precheck.go to use granular error codes

**Files:**
- Modify: `pkg/executor/importer/precheck.go`
- Modify: `pkg/executor/importer/precheck_test.go`

- [ ] **Step 1: Update precheck_test.go to expect new error codes**

In `TestCheckRequirements` (`precheck_test.go`), update the error assertions. The test currently checks `ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)` in multiple places:

1. Line 137: Active job check → change to `exeerrors.ErrLoadDataActiveJobExists`
2. Line 143: File size = 0 → change to `exeerrors.ErrLoadDataNoFilesMatched`
3. Line 153: Non-empty table → change to `exeerrors.ErrLoadDataTargetTableNotEmpty`
4. Line 197: PiTR running → change to `exeerrors.ErrLoadDataPiTRRunning`
5. Line 212: CDC running → change to `exeerrors.ErrLoadDataCDCRunning`
6. Line 227: Unsupported cloud storage scheme → change to `exeerrors.ErrLoadDataCloudStorageUnsupported` (was `ErrorContains`)

Specific changes:

```go
// Line 137: was ErrLoadDataPreCheckFailed
require.ErrorIs(t, err, exeerrors.ErrLoadDataActiveJobExists)
require.ErrorContains(t, err, "active IMPORT INTO job on table")

// Line 143: was ErrLoadDataPreCheckFailed
require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session()), exeerrors.ErrLoadDataNoFilesMatched)

// Line 153: was ErrLoadDataPreCheckFailed
require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session()), exeerrors.ErrLoadDataTargetTableNotEmpty)

// Line 197: was ErrLoadDataPreCheckFailed
require.ErrorIs(t, err, exeerrors.ErrLoadDataPiTRRunning)
require.ErrorContains(t, err, "PiTR log streaming")

// Line 212: was ErrLoadDataPreCheckFailed
require.ErrorIs(t, err, exeerrors.ErrLoadDataCDCRunning)
require.ErrorContains(t, err, "CDC changefeed")

// Line 227: was ErrorContains with "unsupported cloud storage uri scheme"
require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session()), exeerrors.ErrLoadDataCloudStorageUnsupported)
```

Note: The existing test for `ErrLoadDataCloudStorageAccessDenied` (8188) uses a mock S3 server that successfully passes the credential check, so we can't easily trigger this error in unit tests. The error path at precheck.go:165 is covered by the code change but cannot be tested without a more elaborate mock setup. This is acceptable — the S3 mock test at line 234 verifies the success path.

- [ ] **Step 2: Run updated tests to verify they fail**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -run TestCheckRequirements -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: FAIL (tests expect new errors, code still uses old ones)

- [ ] **Step 3: Update precheck.go to use new error codes**

Replace each `ErrLoadDataPreCheckFailed` usage in `precheck.go`:

1. **Line 57** (active job): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("there is active job on the target table already")
// NEW:
return exeerrors.ErrLoadDataActiveJobExists.FastGenByArgs(e.Plan.DBName, e.Plan.TableInfo.Name.O)
```

2. **Line 82** (no files matched): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("No file matched, or the file is empty. Please provide a valid file location.")
// NEW:
return exeerrors.ErrLoadDataNoFilesMatched.FastGenByArgs(e.Path)
```

3. **Line 99** (table not empty): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("target table is not empty")
// NEW:
return exeerrors.ErrLoadDataTargetTableNotEmpty.FastGenByArgs(e.DBName, e.Table.Meta().Name.O)
```

4. **Line 121** (PiTR running): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(fmt.Sprintf("found PiTR log streaming task(s): %v,", names))
// NEW:
return exeerrors.ErrLoadDataPiTRRunning.FastGenByArgs(fmt.Sprintf("%v", names))
```

5. **Line 130** (CDC running): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(nameSet.MessageToUser())
// NEW:
return exeerrors.ErrLoadDataCDCRunning.FastGenByArgs(nameSet.MessageToUser())
```

6. **Line 150** (unsupported cloud storage scheme): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("unsupported cloud storage uri scheme: " + cloudStorageURL.Scheme)
// NEW:
return exeerrors.ErrLoadDataCloudStorageUnsupported.FastGenByArgs(cloudStorageURL.Scheme)
```

7. **Line 165** (cloud storage access denied): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("check cloud storage uri access: " + err.Error())
// NEW:
return exeerrors.ErrLoadDataCloudStorageAccessDenied.FastGenByArgs(e.Plan.CloudStorageURI, err.Error())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -run TestCheckRequirements -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/executor/importer/precheck.go pkg/executor/importer/precheck_test.go
git commit -m "importer: replace ErrLoadDataPreCheckFailed with granular error codes in precheck"
```

## Chunk 3: File Access Phase Call Site Updates

### Task 5: Update import.go to use granular file access error codes

**Files:**
- Modify: `pkg/executor/importer/import.go`

- [ ] **Step 1: Update InitDataFiles() error codes**

In `InitDataFiles()`, replace overloaded `ErrLoadDataInvalidURI` and `ErrLoadDataCantRead` usages:

1. **Line 1391-1393** (non-absolute path): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
    "file location should be absolute path when import from server disk")
// NEW:
return exeerrors.ErrLoadDataInvalidFilePath.GenWithStackByArgs(e.Path,
    "file location should be absolute path when import from server disk")
```

2. **Line 1397-1399** (unsupported suffix): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
    "the file suffix is not supported when import from server disk")
// NEW:
return exeerrors.ErrLoadDataInvalidFilePath.GenWithStackByArgs(e.Path,
    "the file suffix is not supported when import from server disk")
```

3. **Line 1401-1406** (directory stat error): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
    err.Error())
// NEW:
return exeerrors.ErrLoadDataInvalidFilePath.GenWithStackByArgs(dir, err.Error())
```

4. **Line 1413-1417** (glob pattern error): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
    "Glob pattern error: "+err2.Error())
// NEW:
return exeerrors.ErrLoadDataGlobPatternInvalid.GenWithStackByArgs(e.Path, err2.Error())
```

5. **Line 1437-1439** (file open failure — single file): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err2), "Please check the file location is correct")
// NEW:
return exeerrors.ErrLoadDataFileOpenFailed.GenWithStackByArgs(fileNameKey, errors.GetErrStackMsg(err2))
```

6. **Line 1444-1446** (seek failure — keep as `ErrLoadDataCantRead` since this is a read/seek I/O error after successful open):
No change needed — this is a narrowed valid use of `ErrLoadDataCantRead`.

7. **Line 1479-1482** (directory walk failure): Replace:
```go
// OLD:
return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err), "failed to walk dir")
// NEW:
return exeerrors.ErrLoadDataDirWalkFailed.GenWithStackByArgs(errors.GetErrStackMsg(err))
```

8. **Line 1641-1644** (file open failure in Opener): Replace:
```go
// OLD:
return nil, exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err2), "Please check the INFILE path is correct")
// NEW:
return nil, exeerrors.ErrLoadDataFileOpenFailed.GenWithStackByArgs(f.Path, errors.GetErrStackMsg(err2))
```

Note: Keep `ErrLoadDataInvalidURI` at lines 1203, 1243, 1251, 1380, 1884 — these are actual URI parsing failures (the narrowed valid usage).

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/executor/importer/...`
Expected: SUCCESS

- [ ] **Step 3: Run existing import tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -run TestInitDataFiles -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: PASS (or update if tests assert on old error codes)

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/importer/import.go
git commit -m "importer: replace overloaded ErrLoadDataInvalidURI and ErrLoadDataCantRead with granular error codes"
```

## Chunk 3b: Execution Phase Wrapping

### Task 6: Replace ErrEncodeKV with granular error detection in chunk_process.go

**Files:**
- Modify: `pkg/executor/importer/chunk_process.go:92,359`
- Modify: `pkg/executor/importer/chunk_process_testkit_test.go:192,219`

The current code uses `common.ErrEncodeKV` from Lightning as a catch-all for all read/encode errors. Replace with granular error detection that distinguishes CSV/SQL syntax errors, row size errors, and generic encode failures. This task combines the original Task 6 (encode errors) and Task 8 (CSV/SQL detection) since they modify the same code region.

**Error detection strategy:** Both CSV and SQL parsers produce errors starting with `"syntax error:"`. We detect this prefix at the IMPORT INTO boundary. Row size errors contain `"size limit"` or `"entry too large"`. Everything else falls through to `ErrLoadDataEncodeKVFailed`.

- [ ] **Step 1: Add a helper function for error classification**

Add a helper function in `chunk_process.go` (before the `readAndEncode` function):

```go
// classifyReadError wraps a ReadRow/encode error with the appropriate granular error code.
// CSV and SQL parsers both produce "syntax error: ..." messages.
func classifyReadError(err error, filename string, offset int64) error {
	errMsg := err.Error()
	switch {
	case strings.HasPrefix(errMsg, "syntax error:"):
		return exeerrors.ErrLoadDataCSVSyntaxError.GenWithStackByArgs(filename, offset, errMsg)
	case strings.Contains(errMsg, "size limit") || strings.Contains(errMsg, "entry too large"):
		return exeerrors.ErrLoadDataRowSizeTooLarge.GenWithStackByArgs(filename, offset)
	default:
		return exeerrors.ErrLoadDataEncodeKVFailed.Wrap(err).GenWithStackByArgs(filename, offset, errMsg)
	}
}
```

Add `"strings"` and `"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"` to imports.

- [ ] **Step 2: Replace line 92 (readAndEncode ReadRow error)**

```go
// OLD:
err = common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(filename, readPos)
// NEW:
err = classifyReadError(err, filename, readPos)
```

- [ ] **Step 3: Replace line 359 (Process encode error)**

```go
// OLD:
err2 := common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(p.chunkName, data.startPos)
// NEW:
err2 := classifyReadError(encodeErr, p.chunkName, data.startPos)
```

- [ ] **Step 4: Update chunk_process_testkit_test.go**

Update the test assertions that check for `common.ErrEncodeKV`:

```go
// Line 192: was common.ErrEncodeKV
require.ErrorIs(t, err2, exeerrors.ErrLoadDataEncodeKVFailed)

// Line 219: was common.ErrEncodeKV
require.ErrorIs(t, processor.Process(ctx), exeerrors.ErrLoadDataEncodeKVFailed)
```

Add `"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"` to test imports.

- [ ] **Step 5: Run tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -run TestChunkProcess -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/executor/importer/chunk_process.go pkg/executor/importer/chunk_process_testkit_test.go
git commit -m "importer: classify read/encode errors with granular error codes (CSV syntax, row size, encode KV)"
```

### Task 7: Update table_import.go to use new checksum error code

**Files:**
- Modify: `pkg/executor/importer/table_import.go:905`

- [ ] **Step 1: Replace ErrChecksumMismatch**

At line 905, replace:
```go
// OLD:
err2 := common.ErrChecksumMismatch.GenWithStackByArgs(
    remoteChecksum.Checksum, localChecksum.Sum(),
    remoteChecksum.TotalKVs, localChecksum.SumKVS(),
    remoteChecksum.TotalBytes, localChecksum.SumSize(),
)
// NEW:
err2 := exeerrors.ErrLoadDataChecksumMismatch.GenWithStackByArgs(
    e.tableName,
)
```

Note: The new error message is simplified — it takes only the table name. The detailed checksum values are useful for debugging but make the message too complex for end users. The detailed values are already logged.

- [ ] **Step 2: Update test assertions**

In `importer_testkit_test.go`:
```go
// Line 99: was common.ErrChecksumMismatch
require.ErrorIs(t, err, exeerrors.ErrLoadDataChecksumMismatch)

// Line 239: was common.ErrChecksumMismatch
require.ErrorIs(t, importer.PostProcess(ctx, tk.Session(), nil, plan, localChecksum, logger), exeerrors.ErrLoadDataChecksumMismatch)
```

- [ ] **Step 3: Run tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -run TestPostProcess -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/importer/table_import.go pkg/executor/importer/importer_testkit_test.go
git commit -m "importer: use ErrLoadDataChecksumMismatch for checksum verification errors"
```

## Chunk 4: Lightning Parser Error Wrapping

### Task 8: Wrap parquet parser errors

**Files:**
- Modify: `pkg/lightning/mydump/parquet_parser.go:247,273,607-609,635-636`

The parquet parser creates errors like `"unsupported parquet type %s"`, `"parquet read column failed"`, and file open failures. Wrap these with the new error codes.

- [ ] **Step 1: Add import for exeerrors**

Add `"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"` to the import block.

- [ ] **Step 2: Wrap unsupported physical type error (line 247)**

```go
// OLD:
return errors.Errorf("unsupported parquet type %s", tp.String())
// NEW:
return exeerrors.ErrLoadDataParquetUnsupportedType.FastGenByArgs(tp.String(), rgp.filePath)
```

Note: Check the exact struct/receiver name — the `rowGroupParser` struct may store the file path as `rgp.filePath` or similar. Look at the struct definition to find the field name. If file path is not available in `rowGroupParser`, thread it through from the caller or use `"unknown"`.

- [ ] **Step 3: Wrap read column failure (line 273)**

```go
// OLD:
return errors.Annotate(err, "parquet read column failed")
// NEW:
return exeerrors.ErrLoadDataParquetReadFailed.Wrap(err).GenWithStackByArgs(rgp.filePath, err.Error())
```

- [ ] **Step 4: Wrap file open/parse failure (line 607-609)**

```go
// OLD:
reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
if err != nil {
    return nil, errors.Trace(err)
}
// NEW:
reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
if err != nil {
    return nil, exeerrors.ErrLoadDataParquetReadFailed.Wrap(err).GenWithStackByArgs(filePath, err.Error())
}
```

Note: Check the variable name for the file path in the `OpenParquetReader` function signature. It may be a parameter or derived from the reader config.

- [ ] **Step 5: Wrap unsupported logical type error (line 635-636)**

```go
// OLD:
return nil, errors.Errorf("unsupported parquet logical type %s", colTypes[i].converted.String())
// NEW:
return nil, exeerrors.ErrLoadDataParquetUnsupportedType.FastGenByArgs(colTypes[i].converted.String(), filePath)
```

- [ ] **Step 6: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/lightning/mydump/...`
Expected: SUCCESS

- [ ] **Step 7: Commit**

```bash
git add pkg/lightning/mydump/parquet_parser.go
git commit -m "lightning: wrap parquet parser errors with specific IMPORT INTO error codes"
```

### Task 9: Wrap parquet decimal errors

**Files:**
- Modify: `pkg/lightning/mydump/parquet_type_converter.go:55`

- [ ] **Step 1: Replace decimal error**

At line 55 (approximate), replace:
```go
// OLD:
return nil, errors.New("invalid parquet decimal byte array")
// NEW:
return nil, exeerrors.ErrLoadDataParquetDecimalError.FastGenByArgs("unknown", "invalid decimal byte array: length is 0")
```

Note: The file path may not be available at this level. Use `"unknown"` as placeholder — the error will be wrapped with context at a higher level.

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/lightning/mydump/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pkg/lightning/mydump/parquet_type_converter.go
git commit -m "lightning: wrap parquet decimal errors with ErrLoadDataParquetDecimalError"
```

### Task 10: Replace compressed parquet error in router.go

**Files:**
- Modify: `pkg/lightning/mydump/router.go:361-362`

- [ ] **Step 1: Replace the error**

At line 361-362, replace:
```go
// OLD:
return errors.Errorf("can't support whole compressed parquet file, should compress parquet files by choosing correct parquet compress writer, path: %s", r.Path)
// NEW:
return exeerrors.ErrLoadDataParquetCompressedFile.FastGenByArgs(r.Path)
```

Add `"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"` to imports.

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/lightning/mydump/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pkg/lightning/mydump/router.go
git commit -m "lightning: use ErrLoadDataParquetCompressedFile for compressed parquet files"
```

### Task 11: Wrap column count mismatch in tidb.go

**Files:**
- Modify: `pkg/lightning/backend/tidb/tidb.go:609,617`

- [ ] **Step 1: Replace column count mismatch errors**

At line 609, replace:
```go
// OLD:
return errors.Errorf("column count mismatch, expected %d, got %d", enc.columnCnt, len(row))
// NEW:
return exeerrors.ErrLoadDataColumnCountMismatch.FastGenByArgs("unknown", enc.columnCnt, len(row))
```

At line 617, replace similarly:
```go
// OLD:
return errors.Errorf("column count mismatch, at most %d but got %d", len(enc.columnIdx), len(row))
// NEW:
return exeerrors.ErrLoadDataColumnCountMismatch.FastGenByArgs("unknown", len(enc.columnIdx), len(row))
```

Add `"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"` to imports.

- [ ] **Step 2: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/lightning/backend/tidb/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pkg/lightning/backend/tidb/tidb.go
git commit -m "lightning: use ErrLoadDataColumnCountMismatch for column count errors"
```

## Chunk 5: Cleanup and Remove Old Code

### Task 12: Remove ErrLoadDataPreCheckFailed

Now that all call sites have been updated, remove the old catch-all error code.

**Files:**
- Modify: `pkg/errno/errcode.go:1085` — remove `ErrLoadDataPreCheckFailed = 8173`
- Modify: `pkg/errno/errname.go` — remove `ErrLoadDataPreCheckFailed` message entry
- Modify: `pkg/util/dbterror/exeerrors/errors.go:105` — remove `ErrLoadDataPreCheckFailed` variable

- [ ] **Step 1: Search for remaining references**

Run: `grep -rn "ErrLoadDataPreCheckFailed" /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes/pkg/`

Expected: Only the three definition files should remain. If any call sites still reference it, update them first.

- [ ] **Step 2: Remove from errcode.go**

Remove the line `ErrLoadDataPreCheckFailed = 8173`.

- [ ] **Step 3: Remove from errname.go**

Remove the line `ErrLoadDataPreCheckFailed: mysql.Message("PreCheck failed: %s", nil),`.

- [ ] **Step 4: Remove from exeerrors/errors.go**

Remove the line `ErrLoadDataPreCheckFailed = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataPreCheckFailed)`.

- [ ] **Step 5: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go build ./pkg/errno/... ./pkg/util/dbterror/exeerrors/... ./pkg/executor/importer/...`
Expected: SUCCESS

- [ ] **Step 6: Run all affected tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -run TestCheckRequirements -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add pkg/errno/errcode.go pkg/errno/errname.go pkg/util/dbterror/exeerrors/errors.go
git commit -m "errno, exeerrors: remove ErrLoadDataPreCheckFailed (replaced by 8182-8188)"
```

## Chunk 6: Validation

### Task 13: Run lint

**Files:** None (validation only)

- [ ] **Step 1: Run make lint**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && make lint`
Expected: PASS (or fix any lint issues)

- [ ] **Step 2: Fix any lint issues found**

If lint reports issues, fix them and commit.

### Task 14: Run broader test suite

- [ ] **Step 1: Run all importer package tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -tags=intest,deadlock ./pkg/executor/importer/...`
Expected: PASS

- [ ] **Step 2: Run lightning mydump tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -tags=intest,deadlock ./pkg/lightning/mydump/...`
Expected: PASS

- [ ] **Step 3: Run lightning backend tests**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && go test -tags=intest,deadlock ./pkg/lightning/backend/tidb/...`
Expected: PASS

### Task 15: Run make bazel_prepare (no new Go files added)

Since we only modified existing files (no new files added/removed), `make bazel_prepare` is not strictly required. However, verify:

- [ ] **Step 1: Check if bazel_prepare is needed**

No new Go source files were added/removed. No `go.mod` changes. No Bazel files changed. `make bazel_prepare` is NOT required.

- [ ] **Step 2: Final commit check**

Run: `cd /mnt/data/joechenrh/pingcap/tidb/worktrees/import-into-error-codes && git log --oneline`
Verify all commits are present.

---

## Implementation Notes

- **Error wrapping strategy**: For CSV/SQL syntax errors, detect at the IMPORT INTO boundary in `chunk_process.go` via error message prefix matching (`"syntax error:"`). For parquet errors, wrap directly in the Lightning parser since parquet errors have distinct types. This balances keeping Lightning CSV/SQL code shared with BR unchanged while still wrapping parquet errors precisely.
- **`FastGenByArgs` vs `GenWithStackByArgs`**: Use `FastGenByArgs` for errors where stack trace is not needed (pre-check phase, most validation). Use `GenWithStackByArgs` for errors where the stack trace helps debugging (file I/O, parsing).
- **File path in error messages**: Some Lightning errors (e.g., `tidb.go` column count, `parquet_type_converter.go` decimal) don't have access to the file path. Use `"unknown"` as placeholder — the error will be wrapped with context at a higher level, or we accept this limitation for now.
- **Test updates**: Existing tests that check `ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)` must be updated to check for the new specific error code. Tests that only check `ErrorContains` for message substrings may still pass without changes.
- **`ErrLoadDataUnknownColumns` (8400)**: Defined but has no call site in current codebase — `common.ErrUnknownColumns` is defined in Lightning but never instantiated. The error code is reserved for future use when CSV header column validation is implemented.
- **`ErrLoadDataSQLSyntaxError` (8194)**: The SQL parser (`parser.go`) produces `"syntax error: ..."` messages identical in prefix to CSV syntax errors. Both are caught by the same `classifyReadError` function in `chunk_process.go`. If disambiguation between CSV and SQL syntax errors is needed later, we can check the parser type in the classification function.

## Post-Implementation Cleanup

After completing the main plan, an audit of pre-existing `ErrLoadData*` error codes identified additional cleanup:

### Dead code removed:
- **ErrLoadParquetFromLocal** (8155): Zero call sites. Superseded by `ErrLoadDataInvalidFilePath` (8189).
- **ErrLoadDataLocalUnsupportedOption** (8172): Zero call sites. Never used.
- **ErrLoadDataPreCheckFailed** (8173): Removed in Task 12 (replaced by 8182-8188).

### Stale test assertions fixed:
- `import_test.go`: 5 assertions updated from old error codes (`ErrLoadDataInvalidURI`, `ErrLoadDataCantRead`) to new granular codes (`ErrLoadDataInvalidFilePath`, `ErrLoadDataFileOpenFailed`, `ErrLoadDataDirWalkFailed`).
- `precheck_test.go` (realtikvtest): 6 assertions updated from `ErrLoadDataPreCheckFailed` to specific codes.

### Additional call site fixed:
- `pkg/dxf/importinto/scheduler.go:283`: Updated from `ErrLoadDataPreCheckFailed` to `ErrLoadDataTargetTableNotEmpty`.

### Pre-existing codes audited and retained:
| Error | Code | Call sites | Reason kept |
|---|---|---|---|
| ErrLoadDataFromServerDisk | 8154 | 1 | LOAD DATA server-disk prohibition |
| ErrLoadDataEmptyPath | 8156 | 1+1 test | Unique (empty INFILE path) |
| ErrLoadDataUnsupportedFormat | 8157 | 1 | Invalid FORMAT option |
| ErrLoadDataInvalidURI | 8158 | 10+7 test | URI parse/scheme errors |
| ErrLoadDataCantAccess | 8159 | 1 | Storage client creation failure |
| ErrLoadDataCantRead | 8160 | 2+4 test | Narrowed to seek/read I/O errors |
| ErrLoadDataWrongFormatConfig | 8162 | 5+2 test | CSV config validation |
| ErrLoadDataUnsupportedOption | 8166 | 6+3 test | Option/context incompatibility |
| ErrLoadDataDuplicateKeyConflict | 8167 | 1 | Duplicate key normalization |
| ErrLoadDataJobNotFound | 8170 | 2+3 test | Job lifecycle |
| ErrLoadDataInvalidOperation | 8171 | 1+2 test | Job state enforcement |
