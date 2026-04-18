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
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// RangeOpener opens a half-open byte range [start, end) on the backing object.
// Implementations must return a reader that yields exactly end-start bytes
// before EOF; the ParallelReader assumes ranges are delivered in full.
type RangeOpener func(ctx context.Context, start, end int64) (io.ReadCloser, error)

// ParallelReader reads a region of a storage object as an in-order byte stream
// by issuing multiple concurrent range reads. It is intended for cases where
// a single-connection sequential read is slower than downstream consumption,
// e.g. S3 GETs at ~34 MiB/s vs. CSV parse+encode at ~57 MiB/s per worker.
//
// Ordering: bytes are delivered to Read() in the same order as a sequential
// read over [0, totalSize). Range workers may complete out of order; a small
// reorder buffer sequences them.
//
// Concurrency bound: at most `concurrency` ranges are in flight. A worker
// waits before fetching block k if the consumer has not yet reached block
// `k - concurrency`, preventing unbounded memory growth on slow consumers.
type ParallelReader struct {
	cancel    context.CancelFunc
	totalSize int64
	blockSize int64

	mu       sync.Mutex
	cond     *sync.Cond
	ready    map[int64][]byte // offset → block bytes
	err      error
	consumed int64 // highest offset whose bytes have been fully handed to Read()

	curBuf []byte // leftover bytes of current block not yet returned
	pos    int64  // next byte to return from Read()

	wg     sync.WaitGroup
	closed atomic.Bool
}

// NewParallelReader returns a reader that reads totalSize bytes starting at
// offset 0 of the object exposed by opener, using `concurrency` parallel range
// fetches of `blockSize` each.
//
// Guarantees:
//   - Read() returns bytes in sequential order from offset 0 to totalSize-1.
//   - At most `concurrency` ranges are outstanding at any time.
//   - Close() cancels all in-flight fetches and waits for workers to exit.
//
// Defaults: concurrency < 1 becomes 1; blockSize < 1 becomes 8 MiB.
func NewParallelReader(parent context.Context, opener RangeOpener, totalSize int64, concurrency, blockSize int) io.ReadCloser {
	if concurrency < 1 {
		concurrency = 1
	}
	if blockSize < 1 {
		blockSize = 8 * 1024 * 1024
	}
	ctx, cancel := context.WithCancel(parent)
	pr := &ParallelReader{
		cancel:    cancel,
		totalSize: totalSize,
		blockSize: int64(blockSize),
		ready:     make(map[int64][]byte),
	}
	pr.cond = sync.NewCond(&pr.mu)

	// nextAssign is the shared block cursor handed out to workers. Each worker
	// atomically grabs a block offset and fetches that range. Block k's offset
	// is k * blockSize.
	var nextAssign atomic.Int64

	for range concurrency {
		pr.wg.Add(1)
		go pr.worker(ctx, opener, &nextAssign, int64(concurrency))
	}
	return pr
}

func (p *ParallelReader) worker(ctx context.Context, opener RangeOpener, nextAssign *atomic.Int64, concurrency int64) {
	defer p.wg.Done()
	for {
		// Claim the next block.
		blockIdx := nextAssign.Add(1) - 1
		start := blockIdx * p.blockSize
		if start >= p.totalSize {
			return
		}
		end := start + p.blockSize
		if end > p.totalSize {
			end = p.totalSize
		}

		// Throttle: do not get more than `concurrency` blocks ahead of the
		// consumer. This bounds the reorder buffer at ~concurrency blocks.
		p.mu.Lock()
		for !p.closed.Load() && p.err == nil && start-p.consumed >= concurrency*p.blockSize {
			p.cond.Wait()
		}
		if p.closed.Load() || p.err != nil {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		rc, err := opener(ctx, start, end)
		if err != nil {
			p.setErr(err)
			return
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			p.setErr(err)
			return
		}
		if int64(len(data)) != end-start {
			p.setErr(io.ErrUnexpectedEOF)
			return
		}

		p.mu.Lock()
		p.ready[start] = data
		p.cond.Broadcast()
		p.mu.Unlock()
	}
}

func (p *ParallelReader) setErr(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Read implements io.Reader.
func (p *ParallelReader) Read(dst []byte) (int, error) {
	for len(p.curBuf) == 0 {
		if p.pos >= p.totalSize {
			return 0, io.EOF
		}
		p.mu.Lock()
		for {
			if p.err != nil {
				err := p.err
				p.mu.Unlock()
				return 0, err
			}
			if p.closed.Load() {
				p.mu.Unlock()
				return 0, io.ErrClosedPipe
			}
			if data, ok := p.ready[p.pos]; ok {
				delete(p.ready, p.pos)
				p.curBuf = data
				p.consumed = p.pos
				// Wake any workers blocked on throttling.
				p.cond.Broadcast()
				p.mu.Unlock()
				break
			}
			p.cond.Wait()
		}
	}
	n := copy(dst, p.curBuf)
	p.curBuf = p.curBuf[n:]
	p.pos += int64(n)
	return n, nil
}

// Close cancels outstanding fetches and waits for workers to return.
// Safe to call multiple times.
func (p *ParallelReader) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.cancel()
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}
