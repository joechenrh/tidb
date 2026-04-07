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
	"encoding/binary"
)

// rangeProperty stores properties of a range:
// - key: the start key of the range.
// - offset: the start offset of the range in the file.
// - size: the size of the range.
// - keys: the number of keys in the range.
type rangeProperty struct {
	firstKey       []byte
	lastKey        []byte
	offset         uint64
	size           uint64
	keys           uint64
	compressedSize uint64 // v1: bytes occupied by this segment's zstd frame in the data file. 0 for v0 props.
}

func (r *rangeProperty) totalSize() uint64 {
	return r.size + r.keys*2*lengthBytes
}

// decodeMultiProps is only used for test.
func decodeMultiProps(data []byte) []*rangeProperty {
	var ret []*rangeProperty
	for len(data) > 0 {
		propLen := int(binary.BigEndian.Uint32(data))
		propBytes := data[4 : 4+propLen]
		rp := decodeProp(propBytes)
		ret = append(ret, rp)
		data = data[4+propLen:]
	}
	return ret
}

func decodeProp(data []byte) *rangeProperty {
	rp := &rangeProperty{}
	n := 0
	keyLen := int(binary.BigEndian.Uint32(data[n : n+4]))
	n += 4
	rp.firstKey = data[n : n+keyLen]
	n += keyLen
	keyLen = int(binary.BigEndian.Uint32(data[n : n+4]))
	n += 4
	rp.lastKey = data[n : n+keyLen]
	n += keyLen
	rp.size = binary.BigEndian.Uint64(data[n : n+8])
	n += 8
	rp.keys = binary.BigEndian.Uint64(data[n : n+8])
	n += 8
	rp.offset = binary.BigEndian.Uint64(data[n : n+8])
	return rp
}

// keyLen * 2 + p.size + p.keys + p.offset
const propertyLengthExceptKeys = 4*2 + 8 + 8 + 8

func encodeMultiProps(buf []byte, props []*rangeProperty) []byte {
	var propLen [4]byte
	for _, p := range props {
		binary.BigEndian.PutUint32(propLen[:],
			uint32(propertyLengthExceptKeys+len(p.firstKey)+len(p.lastKey)))
		buf = append(buf, propLen[:4]...)
		buf = encodeProp(buf, p)
	}
	return buf
}

func encodeProp(buf []byte, r *rangeProperty) []byte {
	var b [8]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(r.firstKey)))
	buf = append(buf, b[:4]...)
	buf = append(buf, r.firstKey...)
	binary.BigEndian.PutUint32(b[:], uint32(len(r.lastKey)))
	buf = append(buf, b[:4]...)
	buf = append(buf, r.lastKey...)
	binary.BigEndian.PutUint64(b[:], r.size)
	buf = append(buf, b[:]...)
	binary.BigEndian.PutUint64(b[:], r.keys)
	buf = append(buf, b[:]...)
	binary.BigEndian.PutUint64(b[:], r.offset)
	buf = append(buf, b[:]...)
	return buf
}

// V1 stat property layout adds a trailing compressedSize uint64:
//
//	[firstKeyLen u32][firstKey][lastKeyLen u32][lastKey]
//	[size u64][keys u64][offset u64][compressedSize u64]
//
// In v1 stat files, prop.offset means PHYSICAL byte offset of the segment's
// zstd frame in the data file (including the 6-byte file header).
const propertyLengthExceptKeysV1 = propertyLengthExceptKeys + 8

func encodePropV1(buf []byte, r *rangeProperty) []byte {
	buf = encodeProp(buf, r)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], r.compressedSize)
	buf = append(buf, b[:]...)
	return buf
}

func decodePropV1(data []byte) *rangeProperty {
	rp := decodeProp(data[:len(data)-8])
	rp.compressedSize = binary.BigEndian.Uint64(data[len(data)-8:])
	return rp
}

// encodeMultiPropsV1 is the v1 counterpart to encodeMultiProps. It writes a
// length-prefixed sequence of v1 props (which carry the trailing
// compressedSize field). Used by v1 writers when finalizing a stat file.
func encodeMultiPropsV1(buf []byte, props []*rangeProperty) []byte {
	var propLen [4]byte
	for _, p := range props {
		body := encodePropV1(nil, p)
		binary.BigEndian.PutUint32(propLen[:], uint32(len(body)))
		buf = append(buf, propLen[:4]...)
		buf = append(buf, body...)
	}
	return buf
}
