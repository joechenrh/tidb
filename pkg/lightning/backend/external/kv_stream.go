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
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// kvStream is a tiny iterator interface satisfied by both KVReader (v0) and
// SegmentKVReader (v1). The merge step only needs NextKV/Close, so this
// interface lets format dispatch happen at construction time and the iterator
// internals stay format-specific.
type kvStream interface {
	NextKV() (key, val []byte, err error)
	Close() error
}

// detectDataFileFormat probes the first fileHeaderLen bytes of a data file
// and reports whether it is a v1 (zstd-compressed) file or a v0 (raw) file.
// It is safe to call on either format because a raw v0 data file begins with
// an 8-byte big-endian key length, whose high 4 bytes can never collide with
// the ASCII "TGSC" magic for any realistic key size (< ~6 PB). Callers use
// this to dispatch to the right kvStream implementation when no companion
// statsReader is in scope.
//
// Returns (fileFormatV0, nil) on any read that is too short, lacks the
// magic, or encounters a non-fatal read error. A propagated error is
// returned only when the underlying storage Open fails hard.
func detectDataFileFormat(ctx context.Context, store storeapi.Storage, name string) (uint8, error) {
	startOffset := int64(0)
	endOffset := int64(fileHeaderLen)
	rd, err := store.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &startOffset,
		EndOffset:   &endOffset,
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer func() { _ = rd.Close() }()
	buf := make([]byte, fileHeaderLen)
	n, readErr := io.ReadFull(rd, buf)
	if readErr != nil || n < fileHeaderLen {
		// Too short or read error: treat as v0. A real empty/corrupt file
		// will surface its error to the KV reader that follows.
		return fileFormatV0, nil
	}
	if version, _, ok := parseFileHeader(buf); ok && version == fileFormatV1 {
		return fileFormatV1, nil
	}
	return fileFormatV0, nil
}

// openKVStream returns a kvStream for the given data file. fileVersion comes
// from the corresponding statsReader (which already inspected the stat file's
// magic header in newStatsReader).
//
// For v1 files, props MUST be the pre-filtered list of segments to read in
// order; SegmentKVReader ranged-GETs each prop's [offset, offset+compressedSize)
// region as one zstd frame. For v0 files, the existing initFileOffset/bufSize
// parameters are passed through to NewKVReader and props is ignored.
func openKVStream(
	ctx context.Context,
	store storeapi.Storage,
	name string,
	fileVersion uint8,
	props []*rangeProperty,
	initFileOffset uint64,
	bufSize int,
) (kvStream, error) {
	switch fileVersion {
	case fileFormatV0:
		return NewKVReader(ctx, name, store, initFileOffset, bufSize)
	case fileFormatV1:
		return newSegmentKVReader(ctx, store, name, props), nil
	default:
		return nil, errors.Errorf("openKVStream: unknown file version %d", fileVersion)
	}
}
