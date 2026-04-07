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
	"context"
	"encoding/binary"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// zstdDecoderPool holds *zstd.Decoder instances for one-shot DecodeAll use.
// Each SegmentKVReader borrows one decoder for its lifetime; we never share
// decoders across goroutines.
var zstdDecoderPool = sync.Pool{
	New: func() any {
		dec, _ := zstd.NewReader(nil)
		return dec
	},
}

// SegmentKVReader yields key/value pairs from a v1 (zstd-compressed)
// global-sort intermediate data file by ranged-GETing one segment at a time
// and decompressing the entire zstd frame in memory before iterating KVs.
//
// It is intentionally standalone: callers (Task 8 onwards) wrap it in a
// kvStream adapter at merge call sites. The reader takes a slice of
// *rangeProperty already filtered for one file, and assumes each prop's
// (offset, compressedSize) describes one self-contained zstd frame.
type SegmentKVReader struct {
	ctx     context.Context
	store   storeapi.Storage
	name    string
	props   []*rangeProperty
	decoder *zstd.Decoder

	segIdx int
	segBuf []byte
	segPos int
}

// newSegmentKVReader returns a reader for the given file and pre-filtered
// props. Caller MUST call Close() so the borrowed decoder is returned to the
// pool.
func newSegmentKVReader(
	ctx context.Context,
	store storeapi.Storage,
	name string,
	props []*rangeProperty,
) *SegmentKVReader {
	return &SegmentKVReader{
		ctx:     ctx,
		store:   store,
		name:    name,
		props:   props,
		decoder: zstdDecoderPool.Get().(*zstd.Decoder),
	}
}

// NextKV yields the next (key, value) pair. Returns (nil, nil, io.EOF) when
// the reader has consumed every prop. The returned key/value slices alias
// into the reader's internal segment buffer and remain valid only until the
// next NextKV call (since the next call may overwrite segBuf when loading a
// new segment); callers that need to retain them must copy.
func (r *SegmentKVReader) NextKV() (key, val []byte, err error) {
	for r.segPos >= len(r.segBuf) {
		if err := r.loadNextSegment(); err != nil {
			return nil, nil, err
		}
	}
	if len(r.segBuf)-r.segPos < 2*lengthBytes {
		return nil, nil, errors.New("SegmentKVReader: truncated KV header")
	}
	keyLen := int(binary.BigEndian.Uint64(r.segBuf[r.segPos : r.segPos+lengthBytes]))
	valLen := int(binary.BigEndian.Uint64(r.segBuf[r.segPos+lengthBytes : r.segPos+2*lengthBytes]))
	r.segPos += 2 * lengthBytes
	if r.segPos+keyLen+valLen > len(r.segBuf) {
		return nil, nil, errors.New("SegmentKVReader: truncated KV body")
	}
	key = r.segBuf[r.segPos : r.segPos+keyLen]
	val = r.segBuf[r.segPos+keyLen : r.segPos+keyLen+valLen]
	r.segPos += keyLen + valLen
	return key, val, nil
}

// loadNextSegment ranged-GETs the next segment, decompresses it into segBuf,
// and resets segPos. Returns io.EOF when there are no more props.
func (r *SegmentKVReader) loadNextSegment() error {
	if r.segIdx >= len(r.props) {
		return io.EOF
	}
	p := r.props[r.segIdx]
	r.segIdx++
	// In v1, every prop MUST carry a non-zero compressedSize. Zero is the v0
	// sentinel and must never be silently treated as raw here.
	if p.compressedSize == 0 {
		return errors.Errorf("SegmentKVReader: v1 prop with zero compressedSize at offset %d", p.offset)
	}
	start := int64(p.offset)
	end := int64(p.offset + p.compressedSize)
	rd, err := r.store.Open(r.ctx, r.name, &storeapi.ReaderOption{
		StartOffset: aws.Int64(start),
		EndOffset:   aws.Int64(end),
	})
	if err != nil {
		return errors.Trace(err)
	}
	frame, err := io.ReadAll(rd)
	_ = rd.Close()
	if err != nil {
		return errors.Annotatef(err, "SegmentKVReader: read segment offset=%d size=%d", p.offset, p.compressedSize)
	}
	r.segBuf, err = r.decoder.DecodeAll(frame, r.segBuf[:0])
	if err != nil {
		return errors.Annotatef(err, "SegmentKVReader: decode segment offset=%d size=%d", p.offset, p.compressedSize)
	}
	r.segPos = 0
	return nil
}

// Close returns the borrowed decoder to the pool. Safe to call multiple times.
func (r *SegmentKVReader) Close() error {
	if r.decoder != nil {
		zstdDecoderPool.Put(r.decoder)
		r.decoder = nil
	}
	return nil
}
