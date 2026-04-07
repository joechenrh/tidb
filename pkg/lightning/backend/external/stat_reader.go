// Copyright 2023 PingCAP, Inc.
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

package external

import (
	"context"
	"encoding/binary"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

type statsReader struct {
	byteReader  *byteReader
	fileVersion uint8
	// pendingHeader holds the first fileHeaderLen bytes of a v0 stat file
	// after the v1-magic probe consumed them. They are virtually replayed
	// before any further byteReader reads so the v0 decoder still sees the
	// original byte stream starting at offset 0. nil for v1 files (where the
	// header is not part of the prop stream).
	pendingHeader []byte
}

func newStatsReader(ctx context.Context, store storeapi.Storage, name string, bufSize int) (*statsReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, 0, 250*1024)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	if err != nil {
		return nil, err
	}
	r := &statsReader{
		byteReader:  br,
		fileVersion: fileFormatV0,
	}
	// Probe the first fileHeaderLen bytes for the v1 magic. byteReader.readNBytes
	// returns a slice that may be invalidated by subsequent reads, so copy the
	// bytes before saving them as pendingHeader for the v0 replay path.
	hdr, perr := br.readNBytes(fileHeaderLen)
	if perr == nil {
		if version, _, ok := parseFileHeader(hdr); ok && version == fileFormatV1 {
			r.fileVersion = fileFormatV1
			return r, nil
		}
		// v0 file: stash a copy of the header bytes so nextProp can serve them
		// before falling through to byteReader. Re-opening the storage reader
		// to seek back to offset 0 would be cleaner, but it would double the
		// number of underlying GETs (see TestMergePropBaseIter), so we replay
		// in-process instead.
		r.pendingHeader = append([]byte(nil), hdr...)
	}
	return r, nil
}

// version returns the on-disk format detected at open time. Callers pair the
// stat-file format with the corresponding data-file reader (KVReader for v0,
// SegmentKVReader for v1).
func (r *statsReader) version() uint8 { return r.fileVersion }

// readNBytes reads n bytes, draining pendingHeader (the bytes consumed by the
// magic probe) before falling through to byteReader. This keeps the v0 path
// byte-identical to opening the file at offset 0.
func (r *statsReader) readNBytes(n int) ([]byte, error) {
	if len(r.pendingHeader) == 0 {
		return r.byteReader.readNBytes(n)
	}
	if n <= len(r.pendingHeader) {
		out := r.pendingHeader[:n:n]
		r.pendingHeader = r.pendingHeader[n:]
		return out, nil
	}
	// Need bytes from both pendingHeader and byteReader. Allocate a fresh
	// buffer so the returned slice is not invalidated by the next byteReader
	// read.
	out := make([]byte, n)
	copy(out, r.pendingHeader)
	rest, err := r.byteReader.readNBytes(n - len(r.pendingHeader))
	if err != nil {
		return nil, err
	}
	copy(out[len(r.pendingHeader):], rest)
	r.pendingHeader = nil
	return out, nil
}

func (r *statsReader) nextProp() (*rangeProperty, error) {
	lenBytes, err := r.readNBytes(4)
	if err != nil {
		return nil, err
	}
	propLen := int(binary.BigEndian.Uint32(lenBytes))
	propBytes, err := r.readNBytes(propLen)
	if err != nil {
		return nil, noEOF(err)
	}
	if r.fileVersion == fileFormatV1 {
		return decodePropV1(propBytes), nil
	}
	return decodeProp(propBytes), nil
}

func (r *statsReader) Close() error {
	return r.byteReader.Close()
}
