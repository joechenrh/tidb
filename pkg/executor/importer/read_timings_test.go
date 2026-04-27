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
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type slowReader struct {
	inner io.Reader
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.inner.Read(p)
}

func TestTimedReadCloserAccumulates(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	slow := &slowReader{inner: src, delay: 5 * time.Millisecond}
	rc := io.NopCloser(slow)

	var dur time.Duration
	wrapped := newTimedReadCloser(rc, &dur)

	buf := make([]byte, 5)
	_, err := wrapped.Read(buf)
	require.NoError(t, err)
	_, err = wrapped.Read(buf)
	require.NoError(t, err)

	// Two reads, each ~5ms -> expect at least 9ms accumulated.
	require.GreaterOrEqual(t, dur, 9*time.Millisecond)
	require.NoError(t, wrapped.Close())
}

func TestReaderTimingsZeroValueUsable(t *testing.T) {
	var rt ReaderTimings
	require.Zero(t, rt.s3Read.Load())
	require.Zero(t, rt.decompress.Load())
}

func TestAtomicTimedReaderAccumulatesToReaderTimings(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	slow := &slowReader{inner: src, delay: 5 * time.Millisecond}
	rc := io.NopCloser(slow)

	var rt ReaderTimings
	wrapped := newAtomicTimedReader(rc, rt.addS3Read)

	buf := make([]byte, 5)
	_, err := wrapped.Read(buf)
	require.NoError(t, err)
	_, err = wrapped.Read(buf)
	require.NoError(t, err)

	require.GreaterOrEqual(t, time.Duration(rt.s3Read.Load()), 9*time.Millisecond)
	require.Zero(t, rt.decompress.Load(), "decompress must not be touched")
	require.NoError(t, wrapped.Close())
}

// nopSeekCloser adds a no-op Close to *bytes.Reader so it satisfies
// io.ReadSeekCloser (bytes.Reader already satisfies io.ReadSeeker).
type nopSeekCloser struct {
	*bytes.Reader
}

func (n *nopSeekCloser) Close() error { return nil }

// slowReadSeekCloser is an io.ReadSeekCloser whose Read sleeps before
// delegating, used to make timing assertions reliable in tests.
type slowReadSeekCloser struct {
	inner *nopSeekCloser
	delay time.Duration
}

func (s *slowReadSeekCloser) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.inner.Read(p)
}

func (s *slowReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return s.inner.Seek(offset, whence)
}

func (s *slowReadSeekCloser) Close() error { return nil }

func TestTimedReadSeekCloserAccumulatesToReaderTimings(t *testing.T) {
	inner := &slowReadSeekCloser{
		inner: &nopSeekCloser{bytes.NewReader([]byte("hello world"))},
		delay: 5 * time.Millisecond,
	}

	var rt ReaderTimings
	wrapped := &timedReadSeekCloser{ReadSeekCloser: inner, add: rt.addS3Read}

	buf := make([]byte, 5)
	_, err := wrapped.Read(buf)
	require.NoError(t, err)
	_, err = wrapped.Read(buf)
	require.NoError(t, err)

	require.GreaterOrEqual(t, time.Duration(rt.s3Read.Load()), 9*time.Millisecond)
	require.Zero(t, rt.decompress.Load(), "decompress must not be touched")
	require.NoError(t, wrapped.Close())
}
