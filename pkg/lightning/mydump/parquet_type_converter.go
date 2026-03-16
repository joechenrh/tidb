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
	"slices"
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

type setter[T parquet.ColumnTypes] func(T, *types.Datum) (bool, error)

var zeroMyDecimal = types.MyDecimal{}

const (
	// maximumDecimalBytes is the maximum byte length allowed to be parsed directly.
	// It guarantees the value can be stored in MyDecimal wordbuf without overflow.
	// That is: floor(log256(10^81-1))
	maximumDecimalBytes = 33
)

func isUnsignedParquetType(ct schema.ConvertedType) bool {
	switch ct {
	case schema.ConvertedTypes.Uint8, schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Uint32, schema.ConvertedTypes.Uint64:
		return true
	default:
		return false
	}
}

func isTargetType(target *model.ColumnInfo, tps ...byte) bool {
	if target == nil {
		return false
	}
	return slices.Contains(tps, target.GetType())
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
	if target == nil {
		return false
	}
	switch target.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString:
		return true
	case mysql.TypeString:
		return !types.IsBinaryStr(&target.FieldType)
	default:
		return false
	}
}

func postCheckString(val types.Datum, targetFlen int, enc charset.Encoding) bool {
	if val.Kind() != types.KindString && val.Kind() != types.KindBytes {
		return false
	}

	b := val.GetBytes()
	if !enc.IsValid(b) {
		return false
	}
	if targetFlen == types.UnspecifiedLength || len(b) <= targetFlen {
		return true
	}
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

func getBoolSetter(target *model.ColumnInfo) setter[bool] {
	skipCast := target != nil && mysql.IsIntegerType(target.GetType())
	return func(val bool, d *types.Datum) (bool, error) {
		if val {
			d.SetUint64(1)
		} else {
			d.SetUint64(0)
		}
		return skipCast, nil
	}
}

func getInt32Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[int32] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		if target != nil && decimalCanSkipCast(converted.decimalMeta, target) {
			targetFlen := target.GetFlen()
			targetDecimal := target.GetDecimal()
			unsigned := mysql.HasUnsignedFlag(target.GetFlag())
			return func(val int32, d *types.Datum) (bool, error) {
				dec := initializeMyDecimal(d)
				err := setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
				if err != nil {
					return false, err
				}
				return postCheckDecimal(*d, targetFlen, targetDecimal, unsigned), nil
			}
		}
		return func(val int32, d *types.Datum) (bool, error) {
			dec := initializeMyDecimal(d)
			return false, setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
		}
	case schema.ConvertedTypes.Date:
		skipCast := isTargetType(target, mysql.TypeDate)
		return func(val int32, d *types.Datum) (bool, error) {
			// Convert days since Unix epoch to time.Time
			t := time.Unix(int64(val)*86400, 0).In(time.UTC)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
			return skipCast, nil
		}
	case schema.ConvertedTypes.TimeMillis:
		skipCast := isTargetType(target, mysql.TypeDate, mysql.TypeDatetime)
		return func(val int32, d *types.Datum) (bool, error) {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return skipCast, nil
		}
	default:
		fromUnsigned := isUnsignedParquetType(converted.converted)
		if target != nil && mysql.IsIntegerType(target.GetType()) {
			toUnsigned := mysql.HasUnsignedFlag(target.GetFlag())
			if fromUnsigned && toUnsigned {
				upperBound := types.IntegerUnsignedUpperBound(target.GetType())
				return func(val int32, d *types.Datum) (bool, error) {
					d.SetUint64(uint64(uint32(val)))
					return uint64(uint32(val)) <= upperBound, nil
				}
			} else if !fromUnsigned && !toUnsigned {
				lowerBound := types.IntegerSignedLowerBound(target.GetType())
				upperBound := types.IntegerSignedUpperBound(target.GetType())
				return func(val int32, d *types.Datum) (bool, error) {
					d.SetInt64(int64(val))
					return int64(val) >= lowerBound && int64(val) <= upperBound, nil
				}
			}
		}
		if fromUnsigned {
			return func(val int32, d *types.Datum) (bool, error) {
				d.SetUint64(uint64(uint32(val)))
				return false, nil
			}
		}
		return func(val int32, d *types.Datum) (bool, error) {
			d.SetInt64(int64(val))
			return false, nil
		}
	}
}

func getInt64Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[int64] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	isTemporalTarget := target != nil && (target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime)
	switch converted.converted {
	case schema.ConvertedTypes.TimeMicros:
		return func(val int64, d *types.Datum) (bool, error) {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return isTemporalTarget, nil
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) (bool, error) {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return isTemporalTarget, nil
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) (bool, error) {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return isTemporalTarget, nil
		}
	case schema.ConvertedTypes.Decimal:
		if target != nil && decimalCanSkipCast(converted.decimalMeta, target) {
			targetFlen := target.GetFlen()
			targetDecimal := target.GetDecimal()
			unsigned := mysql.HasUnsignedFlag(target.GetFlag())
			return func(val int64, d *types.Datum) (bool, error) {
				dec := initializeMyDecimal(d)
				err := setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
				if err != nil {
					return false, err
				}
				return postCheckDecimal(*d, targetFlen, targetDecimal, unsigned), nil
			}
		}
		return func(val int64, d *types.Datum) (bool, error) {
			dec := initializeMyDecimal(d)
			return false, setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
		}
	default:
		fromUnsigned := isUnsignedParquetType(converted.converted)
		if target != nil && mysql.IsIntegerType(target.GetType()) {
			toUnsigned := mysql.HasUnsignedFlag(target.GetFlag())
			if fromUnsigned && toUnsigned {
				upperBound := types.IntegerUnsignedUpperBound(target.GetType())
				return func(val int64, d *types.Datum) (bool, error) {
					d.SetUint64(uint64(val))
					return uint64(val) <= upperBound, nil
				}
			} else if !fromUnsigned && !toUnsigned {
				lowerBound := types.IntegerSignedLowerBound(target.GetType())
				upperBound := types.IntegerSignedUpperBound(target.GetType())
				return func(val int64, d *types.Datum) (bool, error) {
					d.SetInt64(val)
					return val >= lowerBound && val <= upperBound, nil
				}
			}
		}
		if fromUnsigned {
			return func(val int64, d *types.Datum) (bool, error) {
				d.SetUint64(uint64(val))
				return false, nil
			}
		}
		return func(val int64, d *types.Datum) (bool, error) {
			d.SetInt64(val)
			return false, nil
		}
	}
}

func getInt96Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[parquet.Int96] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	skipCast := isTargetType(target, mysql.TypeDate, mysql.TypeDatetime)
	return func(val parquet.Int96, d *types.Datum) (bool, error) {
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
		if converted.IsAdjustedToUTC {
			t = t.In(loc)
		}
		setTemporalDatum(t, d, temporalType, temporalFSP)
		return skipCast, nil
	}
}

func getFloat32Setter(target *model.ColumnInfo) setter[float32] {
	skipCast := isTargetType(target, mysql.TypeFloat)
	return func(val float32, d *types.Datum) (bool, error) {
		d.SetFloat32(val)
		return skipCast, nil
	}
}

func getFloat64Setter(target *model.ColumnInfo) setter[float64] {
	skipCast := isTargetType(target, mysql.TypeDouble)
	return func(val float64, d *types.Datum) (bool, error) {
		d.SetFloat64(val)
		return skipCast, nil
	}
}

func getByteArraySetter(converted *columnType, target *model.ColumnInfo) setter[parquet.ByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.UTF8:
		if stringCanSkipCast(target) {
			targetFlen := target.GetFlen()
			enc := charset.FindEncoding(target.GetCharset())
			collation := target.GetCollate()
			return func(val parquet.ByteArray, d *types.Datum) (bool, error) {
				d.SetBytesAsString(val, collation, 0)
				return postCheckString(*d, targetFlen, enc), nil
			}
		}
		return func(val parquet.ByteArray, d *types.Datum) (bool, error) {
			d.SetBytesAsString(val, "binary", 0)
			return false, nil
		}
	case schema.ConvertedTypes.Decimal:
		if target != nil && decimalCanSkipCast(converted.decimalMeta, target) {
			targetFlen := target.GetFlen()
			targetDecimal := target.GetDecimal()
			unsigned := mysql.HasUnsignedFlag(target.GetFlag())
			return func(val parquet.ByteArray, d *types.Datum) (bool, error) {
				err := setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
				if err != nil {
					return false, err
				}
				return postCheckDecimal(*d, targetFlen, targetDecimal, unsigned), nil
			}
		}
		return func(val parquet.ByteArray, d *types.Datum) (bool, error) {
			return false, setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	default:
		return func(val parquet.ByteArray, d *types.Datum) (bool, error) {
			d.SetBytesAsString(val, "binary", 0)
			return false, nil
		}
	}
}

func getFixedLenByteArraySetter(converted *columnType, target *model.ColumnInfo) setter[parquet.FixedLenByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.UTF8:
		if target != nil && stringCanSkipCast(target) {
			targetFlen := target.GetFlen()
			enc := charset.FindEncoding(target.GetCharset())
			collation := target.GetCollate()
			return func(val parquet.FixedLenByteArray, d *types.Datum) (bool, error) {
				d.SetBytesAsString(val, collation, 0)
				return postCheckString(*d, targetFlen, enc), nil
			}
		}
		return func(val parquet.FixedLenByteArray, d *types.Datum) (bool, error) {
			d.SetBytesAsString(val, "binary", 0)
			return false, nil
		}
	case schema.ConvertedTypes.Decimal:
		if target != nil && decimalCanSkipCast(converted.decimalMeta, target) {
			targetFlen := target.GetFlen()
			targetDecimal := target.GetDecimal()
			unsigned := mysql.HasUnsignedFlag(target.GetFlag())
			return func(val parquet.FixedLenByteArray, d *types.Datum) (bool, error) {
				err := setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
				if err != nil {
					return false, err
				}
				return postCheckDecimal(*d, targetFlen, targetDecimal, unsigned), nil
			}
		}
		return func(val parquet.FixedLenByteArray, d *types.Datum) (bool, error) {
			return false, setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	default:
		return func(val parquet.FixedLenByteArray, d *types.Datum) (bool, error) {
			d.SetBytesAsString(val, "binary", 0)
			return false, nil
		}
	}
}
