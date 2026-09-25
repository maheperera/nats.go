// Copyright 2012-2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"sync"
	"sync/atomic"
)

// BufferPool defines an interface for managing and recycling byte slices
// used for incoming message payloads.
type BufferPool interface {
	// Get returns a byte slice with at least the requested capacity.
	Get(capacity int) []byte
	// Put returns a byte slice to the pool for reuse.
	Put(buf []byte)
}

// DefaultTierSizes provides the standard size tiers used by TieredBufferPool:
// 4 KB, 32 KB, 128 KB, 512 KB, 2 MB, 8 MB.
var DefaultTierSizes = []int{
	4 * 1024,        // 4 KB  - small messages, control/RPC payloads
	32 * 1024,       // 32 KB - default NATS read buffer size
	128 * 1024,      // 128 KB - medium payloads
	512 * 1024,      // 512 KB - large protobuf/JSON bundles
	2 * 1024 * 1024, // 2 MB  - very large payloads
	8 * 1024 * 1024, // 8 MB  - maximum pooled payload size
}

type bufferTier struct {
	size int
	ch   chan []byte
}

// TieredBufferPool is a size-segregated channel-based buffer pool.
// It holds channels of byte slices pre-allocated to various capacity tiers,
// preventing small and large messages from competing for the same buffers.
type TieredBufferPool struct {
	tiers    []bufferTier
	poolSize int
	reused   uint64
	alloc    uint64
	released uint64
	dropped  uint64
}

// NewTieredBufferPool creates a TieredBufferPool with default size tiers
// and poolSize buffers per tier. If poolSize <= 0, a default of 256 is used.
func NewTieredBufferPool(poolSize int) *TieredBufferPool {
	return NewTieredBufferPoolWithSizes(poolSize, DefaultTierSizes)
}

// NewTieredBufferPoolWithSizes creates a TieredBufferPool with custom tier sizes.
func NewTieredBufferPoolWithSizes(poolSize int, sizes []int) *TieredBufferPool {
	if poolSize <= 0 {
		poolSize = 256
	}
	tiers := make([]bufferTier, len(sizes))
	for i, sz := range sizes {
		tiers[i] = bufferTier{
			size: sz,
			ch:   make(chan []byte, poolSize),
		}
	}
	return &TieredBufferPool{
		tiers:    tiers,
		poolSize: poolSize,
	}
}

// Get returns a byte slice of length capacity and capacity at least tier.size.
func (p *TieredBufferPool) Get(capacity int) []byte {
	for i := range p.tiers {
		if capacity <= p.tiers[i].size {
			select {
			case b := <-p.tiers[i].ch:
				atomic.AddUint64(&p.reused, 1)
				return b[:capacity]
			default:
				atomic.AddUint64(&p.alloc, 1)
				return make([]byte, capacity, p.tiers[i].size)
			}
		}
	}
	// Exceeds highest tier, allocate directly
	atomic.AddUint64(&p.alloc, 1)
	return make([]byte, capacity)
}

// Put returns a buffer to the appropriate tier based on its capacity.
// If the tier's channel is full, the buffer is dropped to be collected by GC.
func (p *TieredBufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}
	c := cap(buf)
	for i := len(p.tiers) - 1; i >= 0; i-- {
		if c >= p.tiers[i].size {
			select {
			case p.tiers[i].ch <- buf[:0]:
				atomic.AddUint64(&p.released, 1)
			default:
				atomic.AddUint64(&p.dropped, 1)
			}
			return
		}
	}
	atomic.AddUint64(&p.dropped, 1)
}

// Stats returns the internal pool statistics.
func (p *TieredBufferPool) Stats() (reused, alloc, released, dropped uint64) {
	return atomic.LoadUint64(&p.reused), atomic.LoadUint64(&p.alloc), atomic.LoadUint64(&p.released), atomic.LoadUint64(&p.dropped)
}

// syncTier holds a sync.Pool for a specific capacity tier.
type syncTier struct {
	size int
	pool sync.Pool
}

// SyncBufferPool is a sync.Pool-based size-classed buffer pool.
// It dynamically scales with goroutines and is automatically collected during idle periods.
type SyncBufferPool struct {
	tiers []syncTier
}

// NewSyncBufferPool creates a SyncBufferPool using DefaultTierSizes.
func NewSyncBufferPool() *SyncBufferPool {
	return NewSyncBufferPoolWithSizes(DefaultTierSizes)
}

// NewSyncBufferPoolWithSizes creates a SyncBufferPool with custom tier sizes.
func NewSyncBufferPoolWithSizes(sizes []int) *SyncBufferPool {
	tiers := make([]syncTier, len(sizes))
	for i, sz := range sizes {
		tiers[i] = syncTier{
			size: sz,
		}
	}
	return &SyncBufferPool{tiers: tiers}
}

// Get returns a byte slice of length capacity and at least tier.size capacity.
func (p *SyncBufferPool) Get(capacity int) []byte {
	for i := range p.tiers {
		if capacity <= p.tiers[i].size {
			if v := p.tiers[i].pool.Get(); v != nil {
				b := v.([]byte)
				return b[:capacity]
			}
			return make([]byte, capacity, p.tiers[i].size)
		}
	}
	return make([]byte, capacity)
}

// Put returns a buffer to the matching sync.Pool tier based on capacity.
func (p *SyncBufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}
	c := cap(buf)
	for i := len(p.tiers) - 1; i >= 0; i-- {
		if c >= p.tiers[i].size {
			p.tiers[i].pool.Put(buf[:0])
			return
		}
	}
}

// WithBufferPool is an Option to enable payload buffer recycling using a tiered buffer pool
// with the specified channel capacity per size class.
func WithBufferPool(size int) Option {
	return func(o *Options) error {
		if size <= 0 {
			size = 256
		}
		o.BufferPoolSize = size
		o.BufferPool = NewTieredBufferPool(size)
		return nil
	}
}

// WithCustomBufferPool is an Option that configures a custom BufferPool implementation.
func WithCustomBufferPool(pool BufferPool) Option {
	return func(o *Options) error {
		o.BufferPool = pool
		return nil
	}
}
