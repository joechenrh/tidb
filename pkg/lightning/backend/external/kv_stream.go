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
