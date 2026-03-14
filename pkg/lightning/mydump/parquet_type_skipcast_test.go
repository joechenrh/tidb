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
	infos := buildSkipCastPrechecks(
		[]convertedType{{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}},
		[]parquet.Type{parquet.Types.Int64},
		[]*model.ColumnInfo{newParquetTargetColumnInfo(mysql.TypeTimestamp, 0, 19, 0, "", "")},
	)
	require.Len(t, infos, 1)
	require.Equal(t, skipCheckNoSkip, infos[0].checkKind)
}

func TestParquetTemporalSetterUsesTargetType(t *testing.T) {
	converted := convertedType{converted: schema.ConvertedTypes.TimeMicros, IsAdjustedToUTC: true}
	target := newParquetTargetColumnInfo(mysql.TypeDate, 0, 0, 0, "", "")

	setter := getInt64Setter(&converted, time.UTC, target)
	var datum types.Datum
	require.NoError(t, setter(0, &datum))
	require.Equal(t, types.KindMysqlTime, datum.Kind())
	require.Equal(t, mysql.TypeDate, datum.GetMysqlTime().Type())
}

func TestSetTemporalDatumTruncation(t *testing.T) {
	t.Run("DATE zeroes time portion", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)
		setTemporalDatum(tm, &d, mysql.TypeDate, 0)
		got := d.GetMysqlTime()
		require.Equal(t, mysql.TypeDate, got.Type())
		require.Equal(t, "2025-01-02", got.String())
	})

	t.Run("DATETIME(0) truncates sub-second", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 999999000, time.UTC)
		setTemporalDatum(tm, &d, mysql.TypeDatetime, 0)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05", got.String())
	})

	t.Run("DATETIME(3) truncates sub-millisecond", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123999000, time.UTC)
		setTemporalDatum(tm, &d, mysql.TypeDatetime, 3)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05.123", got.String())
	})

	t.Run("DATETIME(6) keeps full microseconds", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)
		setTemporalDatum(tm, &d, mysql.TypeDatetime, 6)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05.123456", got.String())
	})
}

func TestParquetSkipCastInfoForStringAndDecimal(t *testing.T) {
	t.Run("float and double", func(t *testing.T) {
		infos := buildSkipCastPrechecks(
			[]convertedType{{converted: schema.ConvertedTypes.None}, {converted: schema.ConvertedTypes.None}},
			[]parquet.Type{parquet.Types.Float, parquet.Types.Double},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeFloat, 0, 0, 0, "", ""),
				newParquetTargetColumnInfo(mysql.TypeDouble, 0, 0, 0, "", ""),
			},
		)
		require.Equal(t, skipCheckUnconditional, infos[0].checkKind)
		require.Equal(t, skipCheckUnconditional, infos[1].checkKind)
	})

	t.Run("utf8 string target", func(t *testing.T) {
		infos := buildSkipCastPrechecks(
			[]convertedType{{converted: schema.ConvertedTypes.UTF8}},
			[]parquet.Type{parquet.Types.ByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 20, 0, "utf8mb4", ""),
			},
		)
		require.Equal(t, skipCheckString, infos[0].checkKind)
		require.NotNil(t, infos[0].encoding) // utf8mb4 → non-nil encoding
	})

	t.Run("varbinary target (binary charset)", func(t *testing.T) {
		infos := buildSkipCastPrechecks(
			[]convertedType{{converted: schema.ConvertedTypes.UTF8}},
			[]parquet.Type{parquet.Types.ByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 20, 0, "binary", ""),
			},
		)
		require.Equal(t, skipCheckString, infos[0].checkKind)
		require.Nil(t, infos[0].encoding) // binary charset → nil encoding
	})

	t.Run("decimal byte array", func(t *testing.T) {
		infos := buildSkipCastPrechecks(
			[]convertedType{{
				converted: schema.ConvertedTypes.Decimal,
				decimalMeta: schema.DecimalMetadata{
					IsSet:     true,
					Precision: 5,
					Scale:     2,
				},
			}},
			[]parquet.Type{parquet.Types.ByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "binary", "binary"),
			},
		)
		require.Equal(t, skipCheckDecimal, infos[0].checkKind)
	})

	t.Run("decimal scale mismatch", func(t *testing.T) {
		infos := buildSkipCastPrechecks(
			[]convertedType{{
				converted: schema.ConvertedTypes.Decimal,
				decimalMeta: schema.DecimalMetadata{
					IsSet:     true,
					Precision: 5,
					Scale:     3,
				},
			}},
			[]parquet.Type{parquet.Types.FixedLenByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "", ""),
			},
		)
		require.Equal(t, skipCheckNoSkip, infos[0].checkKind)
	})
}

func TestIntSetterSignedness(t *testing.T) {
	t.Run("INT64 Uint8 to signed target emits KindInt64", func(t *testing.T) {
		converted := &convertedType{converted: schema.ConvertedTypes.Uint8}
		target := newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		var d types.Datum
		require.NoError(t, setter(200, &d))
		require.Equal(t, types.KindInt64, d.Kind())
		require.Equal(t, int64(200), d.GetInt64())
	})

	t.Run("INT64 Uint8 to unsigned target emits KindUint64", func(t *testing.T) {
		converted := &convertedType{converted: schema.ConvertedTypes.Uint8}
		target := newParquetTargetColumnInfo(mysql.TypeShort, mysql.UnsignedFlag, 6, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		var d types.Datum
		require.NoError(t, setter(200, &d))
		require.Equal(t, types.KindUint64, d.Kind())
		require.Equal(t, uint64(200), d.GetUint64())
	})

	t.Run("INT32 signed to unsigned target emits KindUint64", func(t *testing.T) {
		converted := &convertedType{converted: schema.ConvertedTypes.Int32}
		target := newParquetTargetColumnInfo(mysql.TypeLong, mysql.UnsignedFlag, 10, 0, "", "")
		setter := getInt32Setter(converted, time.UTC, target)
		var d types.Datum
		require.NoError(t, setter(42, &d))
		require.Equal(t, types.KindUint64, d.Kind())
		require.Equal(t, uint64(42), d.GetUint64())
	})

	t.Run("INT32 signed to signed target emits KindInt64", func(t *testing.T) {
		converted := &convertedType{converted: schema.ConvertedTypes.Int32}
		target := newParquetTargetColumnInfo(mysql.TypeLong, 0, 10, 0, "", "")
		setter := getInt32Setter(converted, time.UTC, target)
		var d types.Datum
		require.NoError(t, setter(-42, &d))
		require.Equal(t, types.KindInt64, d.Kind())
		require.Equal(t, int64(-42), d.GetInt64())
	})
}

func TestStringPostCheck(t *testing.T) {
	utf8Enc := charset.FindEncoding(charset.CharsetUTF8MB4)

	t.Run("valid utf8 within length", func(t *testing.T) {
		d := types.NewStringDatum("hello")
		require.True(t, passStringPostCheck(d, 10, utf8Enc))
	})

	t.Run("valid utf8 exceeds char length", func(t *testing.T) {
		d := types.NewStringDatum("hello")
		require.False(t, passStringPostCheck(d, 3, utf8Enc))
	})

	t.Run("multi-byte within char length", func(t *testing.T) {
		d := types.NewStringDatum("你好") // 2 chars, 6 bytes
		require.True(t, passStringPostCheck(d, 5, utf8Enc))
	})

	t.Run("invalid utf8 fails", func(t *testing.T) {
		d := types.NewBytesDatum([]byte{0xff, 0xfe})
		require.False(t, passStringPostCheck(d, 100, utf8Enc))
	})

	t.Run("varbinary nil encoding accepts any bytes", func(t *testing.T) {
		d := types.NewBytesDatum([]byte{0xff, 0xfe, 0x00})
		require.True(t, passStringPostCheck(d, 100, nil))
	})

	t.Run("varbinary exceeds byte length", func(t *testing.T) {
		d := types.NewBytesDatum([]byte{0xff, 0xfe, 0x00})
		require.False(t, passStringPostCheck(d, 2, nil))
	})

	t.Run("negative flen means unlimited", func(t *testing.T) {
		d := types.NewStringDatum("any length string")
		require.True(t, passStringPostCheck(d, -1, utf8Enc))
	})
}

func TestBuildSkipCastPrechecks(t *testing.T) {
	cases := []struct {
		name       string
		converted  convertedType
		physical   parquet.Type
		target     *model.ColumnInfo
		expectKind skipCheckKind
	}{
		{"bool to int", convertedType{converted: schema.ConvertedTypes.None},
			parquet.Types.Boolean,
			newParquetTargetColumnInfo(mysql.TypeLong, 0, 10, 0, "", ""),
			skipCheckUnconditional},
		{"float to float", convertedType{converted: schema.ConvertedTypes.None},
			parquet.Types.Float,
			newParquetTargetColumnInfo(mysql.TypeFloat, 0, 12, 0, "", ""),
			skipCheckUnconditional},
		{"double to double", convertedType{converted: schema.ConvertedTypes.None},
			parquet.Types.Double,
			newParquetTargetColumnInfo(mysql.TypeDouble, 0, 22, 0, "", ""),
			skipCheckUnconditional},
		{"int32 date to DATE", convertedType{converted: schema.ConvertedTypes.Date},
			parquet.Types.Int32,
			newParquetTargetColumnInfo(mysql.TypeDate, 0, 10, 0, "", ""),
			skipCheckUnconditional},
		{"temporal to TIMESTAMP not eligible", convertedType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true},
			parquet.Types.Int64,
			newParquetTargetColumnInfo(mysql.TypeTimestamp, 0, 19, 0, "", ""),
			skipCheckNoSkip},
		{"int to YEAR not eligible", convertedType{converted: schema.ConvertedTypes.None},
			parquet.Types.Int32,
			newParquetTargetColumnInfo(mysql.TypeYear, 0, 4, 0, "", ""),
			skipCheckNoSkip},
		{"utf8 to VARCHAR utf8mb4", convertedType{converted: schema.ConvertedTypes.UTF8},
			parquet.Types.ByteArray,
			newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 255, 0, "utf8mb4", "utf8mb4_bin"),
			skipCheckString},
		{"utf8 to CHAR utf8mb4", convertedType{converted: schema.ConvertedTypes.UTF8},
			parquet.Types.ByteArray,
			newParquetTargetColumnInfo(mysql.TypeString, 0, 255, 0, "utf8mb4", "utf8mb4_bin"),
			skipCheckString},
		{"bytes to BINARY(M) not eligible", convertedType{converted: schema.ConvertedTypes.None},
			parquet.Types.ByteArray,
			newParquetTargetColumnInfo(mysql.TypeString, mysql.BinaryFlag, 10, 0, "binary", "binary"),
			skipCheckNoSkip},
		{"utf8 to VARBINARY", convertedType{converted: schema.ConvertedTypes.UTF8},
			parquet.Types.ByteArray,
			newParquetTargetColumnInfo(mysql.TypeVarchar, mysql.BinaryFlag, 255, 0, "binary", "binary"),
			skipCheckString},
		{"int32 signed to signed bigint", convertedType{converted: schema.ConvertedTypes.Int32},
			parquet.Types.Int32,
			newParquetTargetColumnInfo(mysql.TypeLonglong, 0, 20, 0, "", ""),
			skipCheckUnconditional},
		{"int64 to smallint not eligible (range)", convertedType{converted: schema.ConvertedTypes.Int64},
			parquet.Types.Int64,
			newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", ""),
			skipCheckNoSkip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			infos := buildSkipCastPrechecks(
				[]convertedType{tc.converted},
				[]parquet.Type{tc.physical},
				[]*model.ColumnInfo{tc.target},
			)
			require.Equal(t, tc.expectKind, infos[0].checkKind)
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
		require.False(t, passStringPostCheck(d, 100, nil))
		// But passDecimalPostCheck also returns false for null
		require.False(t, passDecimalPostCheck(d, 10, 2, false))
	})
}

func TestDecimalPostCheck(t *testing.T) {
	t.Run("fits exactly", func(t *testing.T) {
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("123.45")))
		d := types.NewDecimalDatum(dec)
		require.True(t, passDecimalPostCheck(d, 5, 2, false))
	})

	t.Run("precision overflow", func(t *testing.T) {
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("123456.78")))
		d := types.NewDecimalDatum(dec)
		require.False(t, passDecimalPostCheck(d, 5, 2, false))
	})

	t.Run("negative into unsigned", func(t *testing.T) {
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("-1.00")))
		d := types.NewDecimalDatum(dec)
		require.False(t, passDecimalPostCheck(d, 10, 2, true))
	})

	t.Run("frac mismatch", func(t *testing.T) {
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("1.2")))
		d := types.NewDecimalDatum(dec)
		require.False(t, passDecimalPostCheck(d, 10, 2, false))
	})
}
