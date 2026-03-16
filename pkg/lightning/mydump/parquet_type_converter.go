// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"encoding/binary"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum) error

type castDecision uint8

const (
	castRequired castDecision = iota
	castSkipAlways
	castCheckString
	castCheckDecimal
)

type columnSkipCastPrecheck struct {
	target        *model.ColumnInfo
	checkKind     castDecision
	targetFlen    int
	targetDecimal int
	unsigned      bool
	encoding      charset.Encoding // pre-resolved for string columns, nil for binary
}

var zeroMyDecimal = types.MyDecimal{}

const (
	// maximumDecimalBytes is the maximum byte length allowed to be parsed directly.
	// It guarantees the value can be stored in MyDecimal wordbuf without overflow.
	// That is: floor(log256(10^81-1))
	maximumDecimalBytes = 33
)

func buildSkipCastPrechecks(
	colTypes []columnType,
	targetCols []*model.ColumnInfo,
) []columnSkipCastPrecheck {
	infos := make([]columnSkipCastPrecheck, len(colTypes))
	for i := range colTypes {
		if targetCols[i] != nil {
			infos[i] = parquetColumnPrecheck(colTypes[i], targetCols[i])
		}
	}
	return infos
}

func parquetColumnPrecheck(
	tp columnType,
	target *model.ColumnInfo,
) columnSkipCastPrecheck {
	info := columnSkipCastPrecheck{
		target:        target,
		targetFlen:    target.GetFlen(),
		targetDecimal: target.GetDecimal(),
		unsigned:      mysql.HasUnsignedFlag(target.GetFlag()),
	}

	// TIMESTAMP always requires cast because of timezone and DST semantics.
	if target.GetType() == mysql.TypeTimestamp {
		return info
	}

	switch tp.physical {
	case parquet.Types.Boolean:
		if mysql.IsIntegerType(target.GetType()) {
			info.checkKind = castSkipAlways
		}
	case parquet.Types.Float:
		if target.GetType() == mysql.TypeFloat {
			info.checkKind = castSkipAlways
		}
	case parquet.Types.Double:
		if target.GetType() == mysql.TypeDouble {
			info.checkKind = castSkipAlways
		}
	case parquet.Types.Int32:
		info = int32CanSkipCast(tp, target, info)
	case parquet.Types.Int64:
		info = int64CanSkipCast(tp, target, info)
	case parquet.Types.Int96:
		if target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime {
			info.checkKind = castSkipAlways
		}
	case parquet.Types.ByteArray, parquet.Types.FixedLenByteArray:
		switch tp.converted {
		case schema.ConvertedTypes.Decimal:
			if decimalCanSkipCast(tp.decimalMeta, target) {
				info.checkKind = castCheckDecimal
			}
		case schema.ConvertedTypes.UTF8:
			if stringCanSkipCast(target) {
				info.checkKind = castCheckString
				if !strings.EqualFold(target.GetCharset(), "binary") {
					info.encoding = charset.FindEncoding(target.GetCharset())
				}
			}
		}
	}
	return info
}

func int32CanSkipCast(
	colTp columnType,
	target *model.ColumnInfo,
	info columnSkipCastPrecheck,
) columnSkipCastPrecheck {
	switch colTp.converted {
	case schema.ConvertedTypes.Date:
		if target.GetType() == mysql.TypeDate {
			info.checkKind = castSkipAlways
		}
	case schema.ConvertedTypes.TimeMillis:
		if target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime {
			info.checkKind = castSkipAlways
		}
	case schema.ConvertedTypes.Decimal:
		if target.GetType() == mysql.TypeNewDecimal && decimalCanSkipCast(colTp.decimalMeta, target) {
			info.checkKind = castCheckDecimal
		}
	case schema.ConvertedTypes.Int8,
		schema.ConvertedTypes.Int16,
		schema.ConvertedTypes.Int32:
		if signedRangeFitsTarget(math.MinInt32, math.MaxInt32, target) {
			info.checkKind = castSkipAlways
		}
	case schema.ConvertedTypes.Uint8,
		schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Uint32:
		if unsignedRangeFitsTarget(math.MaxUint32, target) {
			info.checkKind = castSkipAlways
		}
	}
	return info
}

func int64CanSkipCast(
	colTp columnType,
	target *model.ColumnInfo,
	info columnSkipCastPrecheck,
) columnSkipCastPrecheck {
	switch colTp.converted {
	case schema.ConvertedTypes.TimeMicros,
		schema.ConvertedTypes.TimestampMillis,
		schema.ConvertedTypes.TimestampMicros:
		if target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime {
			info.checkKind = castSkipAlways
		}
	case schema.ConvertedTypes.Decimal:
		if decimalCanSkipCast(colTp.decimalMeta, target) {
			info.checkKind = castCheckDecimal
		}
	case schema.ConvertedTypes.Int8, schema.ConvertedTypes.Int16,
		schema.ConvertedTypes.Int32, schema.ConvertedTypes.Int64:
		if signedRangeFitsTarget(math.MinInt64, math.MaxInt64, target) {
			info.checkKind = castSkipAlways
		}
	case schema.ConvertedTypes.Uint8, schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Uint32, schema.ConvertedTypes.Uint64:
		if unsignedRangeFitsTarget(math.MaxUint64, target) {
			info.checkKind = castSkipAlways
		}
	}
	return info
}

func signedRangeFitsTarget(sourceMin int64, sourceMax int64, target *model.ColumnInfo) bool {
	if !mysql.IsIntegerType(target.GetType()) || mysql.HasUnsignedFlag(target.GetFlag()) {
		return false
	}
	minValue, maxValue, ok := signedIntTypeRange(target.GetType())
	return ok && sourceMin >= minValue && sourceMax <= maxValue
}

func unsignedRangeFitsTarget(sourceMax uint64, target *model.ColumnInfo) bool {
	if !mysql.IsIntegerType(target.GetType()) {
		return false
	}
	if mysql.HasUnsignedFlag(target.GetFlag()) {
		_, maxValue, ok := unsignedIntTypeRange(target.GetType())
		return ok && sourceMax <= maxValue
	}
	_, maxValue, ok := signedIntTypeRange(target.GetType())
	return ok && sourceMax <= uint64(maxValue)
}

func signedIntTypeRange(tp byte) (minValue int64, maxValue int64, ok bool) {
	if !mysql.IsIntegerType(tp) {
		return 0, 0, false
	}
	return types.IntegerSignedLowerBound(tp), types.IntegerSignedUpperBound(tp), true
}

func unsignedIntTypeRange(tp byte) (minValue uint64, maxValue uint64, ok bool) {
	if !mysql.IsIntegerType(tp) {
		return 0, 0, false
	}
	return 0, types.IntegerUnsignedUpperBound(tp), true
}

func decimalCanSkipCast(decimalMeta schema.DecimalMetadata, target *model.ColumnInfo) bool {
	if target.GetType() != mysql.TypeNewDecimal || target.GetFlen() <= 0 || target.GetDecimal() < 0 {
		return false
	}
	if !decimalMeta.IsSet {
		return false
	}
	if int(decimalMeta.Scale) != target.GetDecimal() {
		return false
	}
	if int(decimalMeta.Precision) > target.GetFlen() {
		return false
	}
	return true
}

func stringCanSkipCast(target *model.ColumnInfo) bool {
	switch target.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString:
		return true
	case mysql.TypeString:
		return !types.IsBinaryStr(&target.FieldType)
	default:
		return false
	}
}

func temporalTargetType(target *model.ColumnInfo, defaultType byte) byte {
	if target == nil {
		return defaultType
	}
	switch target.GetType() {
	case mysql.TypeDate:
		return mysql.TypeDate
	case mysql.TypeDatetime:
		return mysql.TypeDatetime
	default:
		return defaultType
	}
}

func temporalTargetFSP(target *model.ColumnInfo, defaultFSP int) int {
	if target == nil {
		return defaultFSP
	}
	switch target.GetType() {
	case mysql.TypeDate:
		return 0
	case mysql.TypeDatetime:
		if target.GetDecimal() < 0 {
			return defaultFSP
		}
		return min(target.GetDecimal(), types.MaxFsp)
	default:
		return defaultFSP
	}
}

var fspTruncateUnit = [7]time.Duration{
	time.Second,
	100 * time.Millisecond,
	10 * time.Millisecond,
	time.Millisecond,
	100 * time.Microsecond,
	10 * time.Microsecond,
	time.Microsecond,
}

func setTemporalDatum(t time.Time, d *types.Datum, tp byte, fsp int) {
	if tp == mysql.TypeDate {
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	} else if fsp < types.MaxFsp {
		t = t.Truncate(fspTruncateUnit[fsp])
	}
	d.SetMysqlTime(types.NewTime(types.FromGoTime(t), tp, fsp))
}

func initializeMyDecimal(d *types.Datum) *types.MyDecimal {
	// reuse existing decimal
	if d.Kind() == types.KindMysqlDecimal {
		dec := d.GetMysqlDecimal()
		*dec = zeroMyDecimal
		return dec
	}

	dec := new(types.MyDecimal)
	d.SetMysqlDecimal(dec)
	return dec
}

func setDatumFromDecimalByte(d *types.Datum, val []byte, scale int) error {
	// Typically it shouldn't happen.
	if len(val) == 0 {
		return errors.New("invalid parquet decimal byte array")
	}

	// Truncate leading zeros in two's complement representation.
	negative := (val[0] & 0x80) != 0
	start := 0
	for ; start < len(val); start++ {
		if negative && val[start] != 0xff || !negative && val[start] != 0x00 {
			break
		}
	}
	// Keep at least one byte.
	start = max(start-1, 0)
	val = val[start:]

	// If the length or scale is too large, fallback to string parsing.
	if len(val) >= maximumDecimalBytes || scale > 81 {
		s := getStringFromParquetByte(val, scale)
		d.SetBytesAsString(s, "utf8mb4_bin", 0)
		return nil
	}

	dec := initializeMyDecimal(d)
	return dec.FromParquetArray(val, scale)
}

func getStringFromParquetByte(rawBytes []byte, scale int) []byte {
	base := uint64(1_000_000_000)
	baseDigits := 9

	negative := (rawBytes[0] & 0x80) != 0
	if negative {
		for i := range rawBytes {
			rawBytes[i] = ^rawBytes[i]
		}
		for i := len(rawBytes) - 1; i >= 0; i-- {
			rawBytes[i]++
			if rawBytes[i] != 0 {
				break
			}
		}
	}

	var (
		s          = make([]byte, 0, 64)
		n          int
		nDigits    int
		startIndex = 0
		endIndex   = len(rawBytes)
	)

	for startIndex < endIndex && rawBytes[startIndex] == 0 {
		startIndex++
	}

	// Convert base-256 bytes to base-10 string representation.
	for startIndex < endIndex {
		var rem uint64
		for i := startIndex; i < endIndex; i++ {
			v := (rem << 8) | uint64(rawBytes[i])
			q := v / base
			rem = v % base
			rawBytes[i] = byte(q)
			if q == 0 && i == startIndex {
				startIndex++
			}
		}

		for range baseDigits {
			s = append(s, byte(48+rem%10))
			n++
			nDigits++
			rem /= 10
			if nDigits == scale {
				s = append(s, '.')
				n++
			}
			if startIndex == endIndex && rem == 0 {
				break
			}
		}
	}

	for nDigits < scale+1 {
		s = append(s, '0')
		n++
		nDigits++
		if nDigits == scale {
			s = append(s, '.')
			n++
		}
	}

	if negative {
		s = append(s, '-')
	}

	// Reverse the string.
	for i := range len(s) / 2 {
		j := len(s) - 1 - i
		s[i], s[j] = s[j], s[i]
	}

	return s
}

func setParquetDecimalFromInt64(
	unscaled int64,
	dec *types.MyDecimal,
	decimalMeta schema.DecimalMetadata,
) error {
	dec.FromInt(unscaled)

	scale := int(decimalMeta.Scale)
	if err := dec.Shift(-scale); err != nil {
		return err
	}

	return dec.Round(dec, scale, types.ModeTruncate)
}

//nolint:all_revive
func getBoolDataSetter(val bool, d *types.Datum) error {
	if val {
		d.SetUint64(1)
	} else {
		d.SetUint64(0)
	}
	return nil
}

func getInt32Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[int32] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		return func(val int32, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) error {
			// Convert days since Unix epoch to time.Time
			t := time.Unix(int64(val)*86400, 0).In(time.UTC)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimeMillis:
		return func(val int32, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	default:
		if target != nil && mysql.HasUnsignedFlag(target.GetFlag()) {
			return func(val int32, d *types.Datum) error {
				d.SetUint64(uint64(uint32(val)))
				return nil
			}
		}
		return func(val int32, d *types.Datum) error {
			d.SetInt64(int64(val))
			return nil
		}
	}
}

func getInt64Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[int64] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	switch converted.converted {
	case schema.ConvertedTypes.TimeMicros:
		return func(val int64, d *types.Datum) error {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) error {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val int64, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
		}
	default:
		if target != nil && !mysql.HasUnsignedFlag(target.GetFlag()) {
			return func(val int64, d *types.Datum) error {
				d.SetInt64(val)
				return nil
			}
		}
		return func(val int64, d *types.Datum) error {
			d.SetUint64(uint64(val))
			return nil
		}
	}
}

// newInt96 is a utility function to create a parquet.Int96 for test,
// where microseconds is the number of microseconds since Unix epoch.
func newInt96(microseconds int64) parquet.Int96 {
	day := uint32(microseconds/(86400*1e6) + 2440588)
	nanoOfDay := uint64(microseconds % (86400 * 1e6) * 1e3)
	var b [12]byte
	binary.LittleEndian.PutUint64(b[:8], nanoOfDay)
	binary.LittleEndian.PutUint32(b[8:], day)
	return parquet.Int96(b)
}

func setInt96Data(
	val parquet.Int96,
	d *types.Datum,
	loc *time.Location,
	adjustToUTC bool,
	targetType byte,
	targetFSP int,
) {
	// FYI: https://github.com/apache/spark/blob/d66a4e82eceb89a274edeb22c2fb4384bed5078b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L171-L178
	// INT96 timestamp layout
	// --------------------------
	// |   64 bit   |   32 bit   |
	// ---------------------------
	// |  nano sec  |  julian day  |
	// ---------------------------
	// NOTE:
	// INT96 is a deprecated type in parquet format to store timestamp, which consists of
	// two parts: the first 8 bytes is the nanoseconds within the day, and the last 4 bytes
	// is the Julian Day (days since noon on January 1, 4713 BC). And it will be converted it to UTC by
	//   julian day - 2440588 (Julian Day of the Unix epoch 1970-01-01 00:00:00)
	// As julian day is decoded as uint32, so if user store a date before 1970-01-01, the converted time will be wrong
	// and possibly to be truncated.
	t := val.ToTime().In(time.UTC)
	if adjustToUTC {
		t = t.In(loc)
	}
	setTemporalDatum(t, d, targetType, targetFSP)
}

func getInt96Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[parquet.Int96] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	return func(val parquet.Int96, d *types.Datum) error {
		setInt96Data(val, d, loc, converted.IsAdjustedToUTC, temporalType, temporalFSP)
		return nil
	}
}

func setFloat32Data(val float32, d *types.Datum) error {
	d.SetFloat32(val)
	return nil
}

func setFloat64Data(val float64, d *types.Datum) error {
	d.SetFloat64(val)
	return nil
}

func getByteArraySetter(converted *columnType) setter[parquet.ByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.ByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.ByteArray, d *types.Datum) error {
			return setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	}

	return nil
}

func postCheckString(val types.Datum, targetFlen int, enc charset.Encoding) bool {
	if val.Kind() != types.KindString && val.Kind() != types.KindBytes {
		return false
	}
	b := val.GetBytes()
	if enc != nil && !enc.IsValid(b) {
		return false
	}
	if targetFlen == types.UnspecifiedLength || len(b) <= targetFlen {
		return true
	}
	if enc == nil {
		// For binary charset (VARBINARY), flen is byte count — already checked above.
		return false
	}
	// For non-binary charsets, flen is character count.
	// CastColumnValue uses utf8.RuneCountInString for all non-BLOB string types
	// regardless of charset (see ProduceStrWithSpecifiedTp in datum.go:1258-1259).
	return utf8.RuneCount(b) <= targetFlen
}

func postCheckDecimal(val types.Datum, targetFlen int, targetDecimal int, unsigned bool) bool {
	if val.Kind() != types.KindMysqlDecimal {
		return false
	}
	dec := val.GetMysqlDecimal()
	if unsigned && dec.IsNegative() && !dec.IsZero() {
		return false
	}
	precision, frac := dec.PrecisionAndFrac()
	if targetFlen > 0 && precision > targetFlen {
		return false
	}
	if targetDecimal >= 0 && frac != targetDecimal {
		return false
	}
	return true
}

func getFixedLenByteArraySetter(converted *columnType) setter[parquet.FixedLenByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			return setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	}

	return nil
}
