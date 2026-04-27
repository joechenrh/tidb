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

package importer

import (
	"io"
	"sync/atomic"
	"time"
)

// ReaderTimings accumulates per-layer time spent inside nested io.Reader calls
// during an encode subtask. Fields are atomic because the reader chain may be
// consumed by a parser goroutine while metrics are read by the encode loop's
// periodic flush on a different goroutine.
type ReaderTimings struct {
	s3Read     atomic.Int64 // time.Duration
	decompress atomic.Int64 // time.Duration
}

func (r *ReaderTimings) addS3Read(d time.Duration)     { r.s3Read.Add(int64(d)) }
func (r *ReaderTimings) addDecompress(d time.Duration) { r.decompress.Add(int64(d)) }

// NewReaderTimings returns a zero-value ReaderTimings pointer. Exported so
// tests and external callers outside this package can drive the timed
// readers without accessing internal fields.
func NewReaderTimings() *ReaderTimings {
	return &ReaderTimings{}
}

// timedReadCloser wraps an io.ReadCloser and accumulates wall time spent
// inside Read into *dur. Close is not timed. Intended for same-goroutine use;
// if you need cross-goroutine accumulation, feed into a ReaderTimings field.
type timedReadCloser struct {
	inner io.ReadCloser
	dur   *time.Duration
}

func newTimedReadCloser(inner io.ReadCloser, dur *time.Duration) *timedReadCloser {
	return &timedReadCloser{inner: inner, dur: dur}
}

func (t *timedReadCloser) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := t.inner.Read(p)
	*t.dur += time.Since(start)
	return n, err
}

func (t *timedReadCloser) Close() error {
	return t.inner.Close()
}

// atomicTimedReader wraps an io.ReadCloser and atomically accumulates wall
// time spent inside Read via a caller-supplied adder. Unlike timedReadCloser
// which writes to a local *time.Duration, this type is safe for use across
// goroutines because the adder (typically bound to a ReaderTimings field)
// uses atomic addition.
type atomicTimedReader struct {
	inner io.ReadCloser
	add   func(time.Duration)
}

func newAtomicTimedReader(inner io.ReadCloser, add func(time.Duration)) *atomicTimedReader {
	return &atomicTimedReader{inner: inner, add: add}
}

func (a *atomicTimedReader) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := a.inner.Read(p)
	a.add(time.Since(start))
	return n, err
}

func (a *atomicTimedReader) Close() error {
	return a.inner.Close()
}

// timedReadSeekCloser wraps an io.ReadSeekCloser for timed-Read accumulation
// via the caller-supplied adder. Seek and Close pass through untimed.
type timedReadSeekCloser struct {
	io.ReadSeekCloser
	add func(time.Duration)
}

func (t *timedReadSeekCloser) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := t.ReadSeekCloser.Read(p)
	t.add(time.Since(start))
	return n, err
}
