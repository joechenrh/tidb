// Copyright 2026 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func newParquetTargetColumnInfo(tp byte, flag uint, flen int, decimal int, charset string, collate string) *model.ColumnInfo {
	col := &model.ColumnInfo{}
	col.SetType(tp)
	col.SetFlag(flag)
	col.SetFlen(flen)
	col.SetDecimal(decimal)
	col.SetCharset(charset)
	col.SetCollate(collate)
	return col
}

func TestParquetSkipCastTimestampAlwaysCast(t *testing.T) {
	// TimestampMicros targeting mysql.TypeTimestamp should return canSkip=false
	converted := &columnType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}
	target := newParquetTargetColumnInfo(mysql.TypeTimestamp, 0, 19, 0, "", "")
	setter := getInt64Setter(converted, time.UTC, target)
	var datum types.Datum
	canSkip, err := setter(0, &datum)
	require.NoError(t, err)
	require.False(t, canSkip)
}

func TestParquetTemporalSetterUsesTargetType(t *testing.T) {
	converted := &columnType{converted: schema.ConvertedTypes.TimeMicros, IsAdjustedToUTC: true}
	target := newParquetTargetColumnInfo(mysql.TypeDate, 0, 0, 0, "", "")

	setter := getInt64Setter(converted, time.UTC, target)
	var datum types.Datum
	canSkip, err := setter(0, &datum)
	require.NoError(t, err)
	require.True(t, canSkip)
	require.Equal(t, types.KindMysqlTime, datum.Kind())
	require.Equal(t, mysql.TypeDate, datum.GetMysqlTime().Type())
}

func TestSetTemporalDatumTruncation(t *testing.T) {
	t.Run("DATE zeroes time portion", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDate, 0)
		got := d.GetMysqlTime()
		require.Equal(t, mysql.TypeDate, got.Type())
		require.Equal(t, "2025-01-02", got.String())
	})

	t.Run("DATETIME(0) truncates sub-second", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 999999000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 0)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05", got.String())
	})

	t.Run("DATETIME(3) truncates sub-millisecond", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123999000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 3)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05.123", got.String())
	})

	t.Run("DATETIME(6) keeps full microseconds", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 6)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05.123456", got.String())
	})
}

func TestParquetSkipCastInfoForStringAndDecimal(t *testing.T) {
	t.Run("float and double", func(t *testing.T) {
		floatTarget := newParquetTargetColumnInfo(mysql.TypeFloat, 0, 0, 0, "", "")
		doubleTarget := newParquetTargetColumnInfo(mysql.TypeDouble, 0, 0, 0, "", "")

		floatSetter := getFloat32Setter(floatTarget)
		var d types.Datum
		canSkip, err := floatSetter(1.5, &d)
		require.NoError(t, err)
		require.True(t, canSkip)

		doubleSetter := getFloat64Setter(doubleTarget)
		canSkip, err = doubleSetter(2.5, &d)
		require.NoError(t, err)
		require.True(t, canSkip)
	})

	t.Run("utf8 string target", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.UTF8}
		target := newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 20, 0, "utf8mb4", "")

		setter := getBytesSetter[parquet.ByteArray](converted, target)
		var d types.Datum
		canSkip, err := setter([]byte("hello"), &d)
		require.NoError(t, err)
		require.True(t, canSkip)
	})

	t.Run("varbinary target (binary charset)", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.UTF8}
		target := newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 20, 0, "binary", "")

		setter := getBytesSetter[parquet.ByteArray](converted, target)
		var d types.Datum
		canSkip, err := setter([]byte("hello"), &d)
		require.NoError(t, err)
		require.True(t, canSkip)
	})

	t.Run("decimal byte array", func(t *testing.T) {
		converted := &columnType{
			converted: schema.ConvertedTypes.Decimal,
			decimalMeta: schema.DecimalMetadata{
				IsSet:     true,
				Precision: 5,
				Scale:     2,
			},
		}
		target := newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "binary", "binary")

		setter := getBytesSetter[parquet.ByteArray](converted, target)
		require.NotNil(t, setter)
		// A small positive decimal: 1.23 = 123 in 2's complement = [0x00, 0x7B]
		var d types.Datum
		canSkip, err := setter([]byte{0x00, 0x7B}, &d)
		require.NoError(t, err)
		require.True(t, canSkip)
	})

	t.Run("decimal scale mismatch", func(t *testing.T) {
		converted := &columnType{
			converted: schema.ConvertedTypes.Decimal,
			decimalMeta: schema.DecimalMetadata{
				IsSet:     true,
				Precision: 5,
				Scale:     3,
			},
		}
		target := newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "", "")

		setter := getBytesSetter[parquet.FixedLenByteArray](converted, target)
		require.NotNil(t, setter)
		// decimalCanSkipCast fails → always returns false
		var d types.Datum
		canSkip, err := setter([]byte{0x00, 0x7B}, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
	})
}

func TestIntSetterSignedness(t *testing.T) {
	t.Run("INT64 Uint8 to signed target: cross-sign, no range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Uint8}
		target := newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(200, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
		// fromUnsigned=true → SetUint64 in fallback
		require.Equal(t, types.KindUint64, d.Kind())
		require.Equal(t, uint64(200), d.GetUint64())
	})

	t.Run("INT64 Uint8 to unsigned target: same-sign, range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Uint8}
		target := newParquetTargetColumnInfo(mysql.TypeShort, mysql.UnsignedFlag, 6, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(200, &d)
		require.NoError(t, err)
		require.True(t, canSkip) // 200 fits in SMALLINT UNSIGNED
		require.Equal(t, types.KindUint64, d.Kind())
		require.Equal(t, uint64(200), d.GetUint64())
	})

	t.Run("INT32 signed to unsigned target: cross-sign, no range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Int32}
		target := newParquetTargetColumnInfo(mysql.TypeLong, mysql.UnsignedFlag, 10, 0, "", "")
		setter := getInt32Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(42, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
		// fromUnsigned=false → SetInt64 in fallback
		require.Equal(t, types.KindInt64, d.Kind())
		require.Equal(t, int64(42), d.GetInt64())
	})

	t.Run("INT32 signed to signed target: same-sign, range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Int32}
		target := newParquetTargetColumnInfo(mysql.TypeLong, 0, 10, 0, "", "")
		setter := getInt32Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(-42, &d)
		require.NoError(t, err)
		require.True(t, canSkip) // -42 fits in INT signed
		require.Equal(t, types.KindInt64, d.Kind())
		require.Equal(t, int64(-42), d.GetInt64())
	})
}

func TestStringPostCheck(t *testing.T) {
	utf8Enc := charset.FindEncoding(charset.CharsetUTF8MB4)

	t.Run("valid utf8 within length", func(t *testing.T) {
		d := types.NewStringDatum("hello")
		require.True(t, stringCheckFunc(d, 10, utf8Enc))
	})

	t.Run("valid utf8 exceeds char length", func(t *testing.T) {
		d := types.NewStringDatum("hello")
		require.False(t, stringCheckFunc(d, 3, utf8Enc))
	})

	t.Run("multi-byte within char length", func(t *testing.T) {
		d := types.NewStringDatum("你好") // 2 chars, 6 bytes
		require.True(t, stringCheckFunc(d, 5, utf8Enc))
	})

	t.Run("invalid utf8 fails", func(t *testing.T) {
		d := types.NewBytesDatum([]byte{0xff, 0xfe})
		require.False(t, stringCheckFunc(d, 100, utf8Enc))
	})

	t.Run("varbinary accepts any bytes", func(t *testing.T) {
		binEnc := charset.FindEncoding("binary")
		d := types.NewBytesDatum([]byte{0xff, 0xfe, 0x00})
		require.True(t, stringCheckFunc(d, 100, binEnc))
	})

	t.Run("varbinary exceeds byte length", func(t *testing.T) {
		binEnc := charset.FindEncoding("binary")
		d := types.NewBytesDatum([]byte{0xff, 0xfe, 0x00})
		require.False(t, stringCheckFunc(d, 2, binEnc))
	})

	t.Run("negative flen means unlimited", func(t *testing.T) {
		d := types.NewStringDatum("any length string")
		require.True(t, stringCheckFunc(d, -1, utf8Enc))
	})
}

func TestBuildSkipCastPrechecks(t *testing.T) {
	cases := []struct {
		name          string
		getSetter     func(target *model.ColumnInfo) func() (bool, error)
		target        *model.ColumnInfo
		expectCanSkip bool
	}{
		{"bool to int",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getBoolSetter(target)
				return func() (bool, error) { return s(true, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeLong, 0, 10, 0, "", ""),
			true},
		{"float to float",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getFloat32Setter(target)
				return func() (bool, error) { return s(1.0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeFloat, 0, 12, 0, "", ""),
			true},
		{"double to double",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getFloat64Setter(target)
				return func() (bool, error) { return s(1.0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeDouble, 0, 22, 0, "", ""),
			true},
		{"int32 date to DATE",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Date}
				s := getInt32Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeDate, 0, 10, 0, "", ""),
			true},
		{"temporal to TIMESTAMP not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}
				s := getInt64Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeTimestamp, 0, 19, 0, "", ""),
			false},
		{"int to YEAR not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.None}
				s := getInt32Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeYear, 0, 4, 0, "", ""),
			false},
		{"utf8 to VARCHAR utf8mb4",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.UTF8}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 255, 0, "utf8mb4", "utf8mb4_bin"),
			true},
		{"utf8 to CHAR utf8mb4",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.UTF8}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeString, 0, 255, 0, "utf8mb4", "utf8mb4_bin"),
			true},
		{"bytes to BINARY(M) not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.None}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeString, mysql.BinaryFlag, 10, 0, "binary", "binary"),
			false},
		{"utf8 to VARBINARY",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.UTF8}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeVarchar, mysql.BinaryFlag, 255, 0, "binary", "binary"),
			true},
		{"int32 signed default to signed bigint fits",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Int32}
				s := getInt32Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(42, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeLonglong, 0, 20, 0, "", ""),
			true},
		{"int64 signed to smallint signed fits",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Int64}
				s := getInt64Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(42, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", ""),
			true},
		{"int64 signed to smallint signed overflow",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Int64}
				s := getInt64Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(40000, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", ""),
			false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := tc.getSetter(tc.target)
			canSkip, err := fn()
			require.NoError(t, err)
			require.Equal(t, tc.expectCanSkip, canSkip)
		})
	}
}

func TestPostCheckNullValues(t *testing.T) {
	t.Run("null passes string post-check via fillSkipCast early exit", func(t *testing.T) {
		// passStringPostCheck returns false for null (wrong kind),
		// but fillSkipCast should handle null before dispatching.
		d := types.Datum{}
		require.True(t, d.IsNull())
		// Null should NOT pass passStringPostCheck directly
		require.False(t, stringCheckFunc(d, 100, nil))
		// getDecimalCheckFunc also returns false for null
		decCheck := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 10, 2, "", ""),
		)
		require.False(t, decCheck(&d))
	})
}

func TestDecimalPostCheck(t *testing.T) {
	t.Run("fits exactly", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 5, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 5, 2, "", ""),
		)
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("123.45")))
		d := types.NewDecimalDatum(dec)
		require.True(t, check(&d))
	})

	t.Run("precision overflow", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 5, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 5, 2, "", ""),
		)
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("123456.78")))
		d := types.NewDecimalDatum(dec)
		require.False(t, check(&d))
	})

	t.Run("negative into unsigned", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, mysql.UnsignedFlag, 10, 2, "", ""),
		)
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("-1.00")))
		d := types.NewDecimalDatum(dec)
		require.False(t, check(&d))
	})

	t.Run("frac mismatch", func(t *testing.T) {
		// Scale=1 but target expects decimal=2 → getDecimalCheckFunc returns nil
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 1},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 10, 2, "", ""),
		)
		require.Nil(t, check)
	})
}
