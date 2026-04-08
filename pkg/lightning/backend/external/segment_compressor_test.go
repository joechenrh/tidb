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
	"encoding/binary"
	"errors"
	"fmt"
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

func TestKeyValueStore_CompressedRoundTrip(t *testing.T) {
	w := &fakeObjectWriter{}
	rc := &rangePropertiesCollector{
		props:        nil,
		currProp:     &rangeProperty{},
		propSizeDist: 32, // small so we get multiple segments
		propKeysDist: 1024,
	}
	c := newSegmentCompressor()
	defer c.release()
	c.physOffset = uint64(fileHeaderLen)

	store := NewKeyValueStore(context.Background(), w, rc)
	store.WithCompressor(c)
	rc.onBoundary = func(p *rangeProperty) error {
		return c.flushSegment(context.Background(), w, p, p.totalSize())
	}

	// Write enough KVs to produce at least 3 segments at propSizeDist=32.
	const nKVs = 64
	wantPairs := make([][2][]byte, 0, nKVs)
	for i := 0; i < nKVs; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d-payload", i)
		require.NoError(t, store.addRawKV(k, v))
		wantPairs = append(wantPairs, [2][]byte{k, v})
	}
	require.NoError(t, store.finish())
	// finish must also flush any trailing partial segment, so the last prop
	// in rc.props gets compressedSize set.
	require.NotEmpty(t, rc.props)
	for i, p := range rc.props {
		require.NotZero(t, p.compressedSize, "prop %d has zero compressedSize after finish()", i)
	}

	// Decode every frame back and concatenate; the result should equal the
	// raw KV stream we expected to write.
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	all := w.buf.Bytes()
	var decoded []byte
	cursor := uint64(0)
	for _, p := range rc.props {
		// p.offset is physical, but in this test the writer started at
		// physOffset = fileHeaderLen (we did not pre-write a header to the
		// fakeObjectWriter), so the first frame is at relative offset 0.
		// Compare relative.
		relStart := p.offset - uint64(fileHeaderLen)
		require.Equal(t, cursor, relStart, "frame %d not contiguous", relStart)
		frame := all[cursor : cursor+p.compressedSize]
		got, err := dec.DecodeAll(frame, nil)
		require.NoError(t, err)
		decoded = append(decoded, got...)
		cursor += p.compressedSize
	}

	// Walk decoded buffer and pull out (key, value) pairs to compare.
	gotPairs := make([][2][]byte, 0, nKVs)
	pos := 0
	for pos < len(decoded) {
		klen := binary.BigEndian.Uint64(decoded[pos : pos+8])
		vlen := binary.BigEndian.Uint64(decoded[pos+8 : pos+16])
		k := decoded[pos+16 : pos+16+int(klen)]
		v := decoded[pos+16+int(klen) : pos+16+int(klen)+int(vlen)]
		gotPairs = append(gotPairs, [2][]byte{k, v})
		pos += 16 + int(klen) + int(vlen)
	}
	require.Equal(t, len(wantPairs), len(gotPairs))
	for i := range wantPairs {
		require.Equal(t, wantPairs[i][0], gotPairs[i][0])
		require.Equal(t, wantPairs[i][1], gotPairs[i][1])
	}
}

// failingWriter is an objectio.Writer that fails after N successful writes.
// Used to pin the "write-before-mutate" invariant in segmentCompressor.flushSegment:
// a failed write MUST NOT advance physOffset or mutate the finalized prop.
type failingWriter struct {
	failAfter int
	written   int
	buf       bytes.Buffer
}

func (f *failingWriter) Write(_ context.Context, p []byte) (int, error) {
	f.written++
	if f.written > f.failAfter {
		return 0, errors.New("injected write error")
	}
	return f.buf.Write(p)
}
func (f *failingWriter) Close(_ context.Context) error { return nil }

func TestSegmentCompressor_WriteErrorPropagates(t *testing.T) {
	c := newSegmentCompressor()
	defer c.release()
	c.physOffset = uint64(fileHeaderLen)

	c.rawBuf = append(c.rawBuf, bytes.Repeat([]byte("a"), 64)...)

	// failAfter=0 means EVERY Write call fails.
	w := &failingWriter{failAfter: 0}
	prop := &rangeProperty{}
	err := c.flushSegment(context.Background(), w, prop, 64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected write error")

	// Post-failure invariants: state MUST NOT have advanced.
	require.EqualValues(t, fileHeaderLen, c.physOffset,
		"physOffset must not advance on a failed write")
	require.Zero(t, prop.compressedSize,
		"prop.compressedSize must not be set on a failed write")
	require.Zero(t, prop.offset,
		"prop.offset must not be set on a failed write")

	// rawBuf MUST still hold the original 64 bytes — a failed flushSegment
	// MUST NOT trim rawBuf, because a future retry (or Close path) would
	// otherwise lose the unencoded bytes.
	require.Equal(t, 64, len(c.rawBuf),
		"rawBuf must not be trimmed on a failed write")

	// No bytes should have been written to the fake writer's buffer because
	// the failing writer returns (0, err) before storing anything.
	require.Equal(t, 0, w.buf.Len(),
		"failing writer must not have accepted any bytes")
}
