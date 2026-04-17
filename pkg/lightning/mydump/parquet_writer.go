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
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// ParquetColumn defines the properties of a column in a Parquet file.
// Used to generate parquet files in tests.
type ParquetColumn struct {
	Name      string
	Type      parquet.Type
	Converted schema.ConvertedType
	TypeLen   int
	Precision int
	Scale     int
	Gen       func(numRows int) (any, []int16)
}

// parquetListEncoding selects which on-wire LIST encoding the test writer emits.
// See validateFloat32ListRoot in parquet_parser.go for the reader-side spec.
type parquetListEncoding int

const (
	// parquetList3LevelOptional is the modern 3-level encoding with optional element:
	//   optional group <name> (LIST) { repeated group list { optional float element } }
	// Leaf maxDef = 3.
	parquetList3LevelOptional parquetListEncoding = iota
	// parquetList3LevelRequired is the 3-level encoding with required element:
	//   optional group <name> (LIST) { repeated group list { required float element } }
	// Leaf maxDef = 2 (same as 2-level, but a different schema shape).
	parquetList3LevelRequired
	// parquetList2Level is the legacy 2-level encoding:
	//   optional group <name> (LIST) { repeated float element }
	// Leaf maxDef = 2.
	parquetList2Level
)

// maxDef returns the leaf max definition level implied by the encoding.
func (e parquetListEncoding) maxDef() int16 {
	switch e {
	case parquetList3LevelOptional:
		return 3
	default:
		return 2
	}
}

type parquetListGen func() (values []float32, defLevels, repLevels []int16)

// parquetListColumn describes one LIST<FLOAT> column to emit in a test file.
type parquetListColumn struct {
	Encoding parquetListEncoding
	Gen      parquetListGen
}

type writeWrapper struct {
	Writer storage.ExternalFileWriter
}

func (*writeWrapper) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (*writeWrapper) Read(_ []byte) (int, error) {
	return 0, nil
}

func (w *writeWrapper) Write(b []byte) (int, error) {
	return w.Writer.Write(context.Background(), b)
}

func (w *writeWrapper) Close() error {
	return w.Writer.Close(context.Background())
}

func getStore(path string) (storage.ExternalStorage, error) {
	s, err := storage.ParseBackend(path, nil)
	if err != nil {
		return nil, err
	}

	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// WriteParquetFile writes a simple Parquet file with the specified columns and number of rows.
// It's used for test and DON'T use this function to generate large Parquet files.
func WriteParquetFile(path, fileName string, pcolumns []ParquetColumn, rows int, addOpts ...parquet.WriterProperty) error {
	s, err := getStore(path)
	if err != nil {
		return err
	}
	writer, err := s.Create(context.Background(), fileName, nil)
	if err != nil {
		return err
	}
	wrapper := &writeWrapper{Writer: writer}

	fields := make([]schema.Node, len(pcolumns))
	opts := make([]parquet.WriterProperty, 0, len(pcolumns)*2)
	for i, pc := range pcolumns {
		typeLen := -1
		if pc.TypeLen > 0 {
			typeLen = pc.TypeLen
		}
		if fields[i], err = schema.NewPrimitiveNodeConverted(
			pc.Name,
			parquet.Repetitions.Optional,
			pc.Type, pc.Converted,
			typeLen, pc.Precision, pc.Scale,
			-1,
		); err != nil {
			return err
		}
		opts = append(opts, parquet.WithDictionaryFor(pc.Name, true))
		opts = append(opts, parquet.WithCompressionFor(pc.Name, compress.Codecs.Snappy))
	}

	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	opts = append(opts, addOpts...)
	props := parquet.NewWriterProperties(opts...)
	pw := file.NewParquetWriter(wrapper, node, file.WithWriterProps(props))
	//nolint: errcheck
	defer pw.Close()

	// Only one row group for simplicity
	rgw := pw.AppendRowGroup()
	//nolint: errcheck
	defer rgw.Close()

	for _, pc := range pcolumns {
		cw, err := rgw.NextColumn()
		if err != nil {
			return err
		}
		vals, defLevel := pc.Gen(rows)

		switch w := cw.(type) {
		case *file.Int96ColumnChunkWriter:
			buf, _ := vals.([]parquet.Int96)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Int64ColumnChunkWriter:
			buf, _ := vals.([]int64)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Float64ColumnChunkWriter:
			buf, _ := vals.([]float64)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Float32ColumnChunkWriter:
			buf, _ := vals.([]float32)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.ByteArrayColumnChunkWriter:
			buf, _ := vals.([]parquet.ByteArray)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Int32ColumnChunkWriter:
			buf, _ := vals.([]int32)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.BooleanColumnChunkWriter:
			buf, _ := vals.([]bool)
			_, err = w.WriteBatch(buf, defLevel, nil)
		default:
			return fmt.Errorf("unsupported column type %T", cw)
		}

		if err != nil {
			return err
		}
		if err := cw.Close(); err != nil {
			return err
		}
	}

	return nil
}

// buildFloat32ListField builds a schema node for a single LIST<float> column
// using the requested on-wire encoding.
func buildFloat32ListField(colName string, enc parquetListEncoding) (schema.Node, error) {
	switch enc {
	case parquetList3LevelOptional, parquetList3LevelRequired:
		elementRep := parquet.Repetitions.Optional
		if enc == parquetList3LevelRequired {
			elementRep = parquet.Repetitions.Required
		}
		element, err := schema.NewPrimitiveNode("element", elementRep, parquet.Types.Float, -1, -1)
		if err != nil {
			return nil, err
		}
		listGroup, err := schema.NewGroupNode("list", parquet.Repetitions.Repeated, []schema.Node{element}, -1)
		if err != nil {
			return nil, err
		}
		return schema.NewGroupNodeConverted(colName, parquet.Repetitions.Optional, []schema.Node{listGroup}, schema.ConvertedTypes.List, -1)
	case parquetList2Level:
		element, err := schema.NewPrimitiveNode("element", parquet.Repetitions.Repeated, parquet.Types.Float, -1, -1)
		if err != nil {
			return nil, err
		}
		return schema.NewGroupNodeConverted(colName, parquet.Repetitions.Optional, []schema.Node{element}, schema.ConvertedTypes.List, -1)
	default:
		return nil, fmt.Errorf("unknown parquet list encoding: %d", enc)
	}
}

// WriteParquetListFile writes a Parquet file made of LIST<float> columns.
// Each column chooses its own encoding (2-level or 3-level). The generator
// must return leaf values together with matching definition and repetition
// level sequences. It is intended for tests only.
func WriteParquetListFile(path, fileName string, columns []parquetListColumn, addOpts ...parquet.WriterProperty) error {
	s, err := getStore(path)
	if err != nil {
		return err
	}
	writer, err := s.Create(context.Background(), fileName, nil)
	if err != nil {
		return err
	}
	wrapper := &writeWrapper{Writer: writer}

	fields := make([]schema.Node, len(columns))
	opts := make([]parquet.WriterProperty, 0, len(columns))
	for i, col := range columns {
		// Column name doesn't matter for current tests.
		colName := fmt.Sprintf("c%d", i)
		fields[i], err = buildFloat32ListField(colName, col.Encoding)
		if err != nil {
			return err
		}
		opts = append(opts, parquet.WithCompressionFor(colName, compress.Codecs.Snappy))
	}

	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	opts = append(opts, addOpts...)
	props := parquet.NewWriterProperties(opts...)
	pw := file.NewParquetWriter(wrapper, node, file.WithWriterProps(props))
	//nolint: errcheck
	defer pw.Close()

	rgw := pw.AppendRowGroup()
	//nolint: errcheck
	defer rgw.Close()

	for i, col := range columns {
		cw, err := rgw.NextColumn()
		if err != nil {
			return err
		}
		w, ok := cw.(*file.Float32ColumnChunkWriter)
		if !ok {
			return fmt.Errorf("parquet list column %d: expected Float32ColumnChunkWriter, got %T", i, cw)
		}
		vals, defLevels, repLevels := col.Gen()
		presentDef := col.Encoding.maxDef()
		expectedValues := 0
		for _, d := range defLevels {
			if d == presentDef {
				expectedValues++
			}
		}
		if len(vals) != expectedValues {
			return fmt.Errorf("parquet list column %d has %d values but expects %d (defLevels=%v)", i, len(vals), expectedValues, defLevels)
		}
		if _, err := w.WriteBatch(vals, defLevels, repLevels); err != nil {
			return err
		}
		if err := cw.Close(); err != nil {
			return err
		}
	}

	return nil
}
