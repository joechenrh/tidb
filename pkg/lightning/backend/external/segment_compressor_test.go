// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"bytes"
	"context"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// fakeObjectWriter accumulates bytes for unit tests.
type fakeObjectWriter struct {
	buf bytes.Buffer
}

func (f *fakeObjectWriter) Write(_ context.Context, p []byte) (int, error) {
	return f.buf.Write(p)
}
func (f *fakeObjectWriter) Close(_ context.Context) error { return nil }

func TestSegmentCompressor_FlushSingleSegment(t *testing.T) {
	c := newSegmentCompressor()
	defer c.release()

	c.physOffset = uint64(fileHeaderLen)
	raw := []byte("hello world hello world hello world")
	c.rawBuf = append(c.rawBuf, raw...)

	w := &fakeObjectWriter{}
	prop := &rangeProperty{}
	err := c.flushSegment(context.Background(), w, prop, uint64(len(raw)))
	require.NoError(t, err)

	require.NotZero(t, prop.compressedSize)
	require.EqualValues(t, fileHeaderLen, prop.offset, "first segment starts right after the header")
	require.Equal(t, prop.compressedSize, c.physOffset-uint64(fileHeaderLen), "physOffset must advance by exactly the compressed-frame size")

	// Decode back the bytes the writer received and check they match.
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(w.buf.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, raw, got)

	// rawBuf is fully consumed.
	require.Empty(t, c.rawBuf)
}

func TestSegmentCompressor_FlushTwoSegments(t *testing.T) {
	c := newSegmentCompressor()
	defer c.release()
	c.physOffset = uint64(fileHeaderLen)

	w := &fakeObjectWriter{}

	seg1 := bytes.Repeat([]byte("ab"), 64)
	seg2 := bytes.Repeat([]byte("cd"), 64)

	c.rawBuf = append(c.rawBuf, seg1...)
	prop1 := &rangeProperty{}
	require.NoError(t, c.flushSegment(context.Background(), w, prop1, uint64(len(seg1))))

	c.rawBuf = append(c.rawBuf, seg2...)
	prop2 := &rangeProperty{}
	require.NoError(t, c.flushSegment(context.Background(), w, prop2, uint64(len(seg2))))

	require.EqualValues(t, fileHeaderLen, prop1.offset)
	require.EqualValues(t, uint64(fileHeaderLen)+prop1.compressedSize, prop2.offset)
	require.NotZero(t, prop1.compressedSize)
	require.NotZero(t, prop2.compressedSize)

	// Decode each frame back independently using the recorded ranges.
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	all := w.buf.Bytes()
	frame1 := all[:prop1.compressedSize]
	frame2 := all[prop1.compressedSize : prop1.compressedSize+prop2.compressedSize]

	got1, err := dec.DecodeAll(frame1, nil)
	require.NoError(t, err)
	require.Equal(t, seg1, got1)

	got2, err := dec.DecodeAll(frame2, nil)
	require.NoError(t, err)
	require.Equal(t, seg2, got2)
}
