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

package external

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func buildStatBlobV0(props []*rangeProperty) []byte {
	return encodeMultiProps(nil, props)
}

func buildStatBlobV1(props []*rangeProperty) []byte {
	var buf bytes.Buffer
	buf.Write(fileHeaderV1Zstd)
	for _, p := range props {
		body := encodePropV1(nil, p)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
		buf.Write(lenBuf[:])
		buf.Write(body)
	}
	return buf.Bytes()
}

func TestStatsReader_DispatchV0(t *testing.T) {
	props := []*rangeProperty{
		{firstKey: []byte("a"), lastKey: []byte("b"), size: 10, keys: 2, offset: 0},
	}
	blob := buildStatBlobV0(props)

	ctx := context.Background()
	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "fake", blob))

	r, err := newStatsReader(ctx, store, "fake", 1024)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	require.EqualValues(t, fileFormatV0, r.version())
	got, err := r.nextProp()
	require.NoError(t, err)
	require.Equal(t, props[0], got)
}

func TestStatsReader_DispatchV1(t *testing.T) {
	props := []*rangeProperty{
		{firstKey: []byte("a"), lastKey: []byte("b"), size: 10, keys: 2, offset: 6, compressedSize: 9},
	}
	blob := buildStatBlobV1(props)

	ctx := context.Background()
	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "fake", blob))

	r, err := newStatsReader(ctx, store, "fake", 1024)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	require.EqualValues(t, fileFormatV1, r.version())
	got, err := r.nextProp()
	require.NoError(t, err)
	require.Equal(t, props[0], got)
}
