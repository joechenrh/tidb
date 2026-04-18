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

package prefetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// makeOpener returns a RangeOpener that serves from an in-memory byte slice,
// optionally with a randomized delay to simulate parallel I/O.
func makeOpener(data []byte, maxJitter time.Duration) RangeOpener {
	return func(_ context.Context, start, end int64) (io.ReadCloser, error) {
		if start < 0 || end > int64(len(data)) || start > end {
			return nil, errors.New("range out of bounds")
		}
		if maxJitter > 0 {
			time.Sleep(time.Duration(rand.Int64N(int64(maxJitter))))
		}
		return io.NopCloser(bytes.NewReader(data[start:end])), nil
	}
}

func TestParallelReader_InOrder(t *testing.T) {
	// Fill a buffer with a deterministic pattern so we can verify byte order.
	const size = 1 << 20 // 1 MiB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	pr := NewParallelReader(context.Background(), makeOpener(data, 0), int64(size), 4, 64*1024)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestParallelReader_InOrderWithJitter(t *testing.T) {
	// With jitter, range fetchers finish out of order. Result must still be in order.
	const size = 2 << 20 // 2 MiB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	pr := NewParallelReader(context.Background(), makeOpener(data, 2*time.Millisecond), int64(size), 8, 32*1024)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestParallelReader_NonAlignedTail(t *testing.T) {
	// File size not a multiple of block size.
	const size = 100_000
	const block = 8 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	pr := NewParallelReader(context.Background(), makeOpener(data, 0), int64(size), 4, block)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestParallelReader_SmallerThanOneBlock(t *testing.T) {
	const size = 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}

	pr := NewParallelReader(context.Background(), makeOpener(data, 0), int64(size), 4, 64*1024)
	defer pr.Close()

	got, err := io.ReadAll(pr)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestParallelReader_ErrorPropagation(t *testing.T) {
	sentinel := errors.New("planned failure")
	var callCount atomic.Int32
	failingOpener := RangeOpener(func(_ context.Context, start, end int64) (io.ReadCloser, error) {
		// Fail the second call (some later block) so the first block succeeds
		// but a later one errors out.
		n := callCount.Add(1)
		if n == 2 {
			return nil, sentinel
		}
		buf := make([]byte, end-start)
		return io.NopCloser(bytes.NewReader(buf)), nil
	})

	pr := NewParallelReader(context.Background(), failingOpener, 1<<20, 2, 64*1024)
	defer pr.Close()

	_, err := io.ReadAll(pr)
	require.ErrorIs(t, err, sentinel)
}

func TestParallelReader_ContextCancelOnClose(t *testing.T) {
	// Opener that blocks until its context is cancelled. Close() should unblock.
	slowOpener := RangeOpener(func(ctx context.Context, _, end int64) (io.ReadCloser, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	pr := NewParallelReader(context.Background(), slowOpener, 1<<20, 2, 64*1024)

	// Close immediately, without reading anything.
	done := make(chan struct{})
	go func() {
		_ = pr.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2 seconds")
	}
}

func TestParallelReader_BoundedOutstanding(t *testing.T) {
	// Verify that at most `concurrency` range fetches are in flight at once,
	// i.e., workers throttle when the consumer hasn't caught up.
	const concurrency = 3
	const block = 64 * 1024
	const size = concurrency * block * 20 // way more blocks than workers

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	trackingOpener := RangeOpener(func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}
		// A small sleep makes the race between workers reliably observable.
		time.Sleep(time.Millisecond)
		return io.NopCloser(bytes.NewReader(data[start:end])), nil
	})

	pr := NewParallelReader(context.Background(), trackingOpener, int64(size), concurrency, block)
	defer pr.Close()

	// Drain slowly to let the throttling kick in.
	buf := make([]byte, 1024)
	total := 0
	for total < size {
		n, err := pr.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, size, total)
	require.LessOrEqual(t, int(maxInFlight.Load()), concurrency,
		"expected at most %d in-flight fetches, saw %d", concurrency, maxInFlight.Load())
}
