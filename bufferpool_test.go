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
	"bytes"
	"fmt"
	"testing"
)

func TestTieredBufferPoolBasic(t *testing.T) {
	pool := NewTieredBufferPool(10)

	testSizes := []struct {
		requested int
		minCap    int
	}{
		{100, 4 * 1024},
		{4 * 1024, 4 * 1024},
		{5 * 1024, 32 * 1024},
		{32 * 1024, 32 * 1024},
		{50 * 1024, 128 * 1024},
		{400 * 1024, 512 * 1024}, // common protobuf bundle size
		{1024 * 1024, 2 * 1024 * 1024},
		{5 * 1024 * 1024, 8 * 1024 * 1024},
		{10 * 1024 * 1024, 10 * 1024 * 1024}, // exceeds max tier
	}

	for _, tc := range testSizes {
		buf := pool.Get(tc.requested)
		if len(buf) != tc.requested {
			t.Fatalf("For requested %d, expected len %d, got %d", tc.requested, tc.requested, len(buf))
		}
		if cap(buf) < tc.minCap {
			t.Fatalf("For requested %d, expected cap at least %d, got %d", tc.requested, tc.minCap, cap(buf))
		}
		pool.Put(buf)
	}

	// Now re-fetch; buffers within tier limits should be reused
	reusedBefore, _, _, _ := pool.Stats()
	buf := pool.Get(400 * 1024)
	reusedAfter, _, _, _ := pool.Stats()
	if reusedAfter <= reusedBefore {
		t.Fatalf("Expected reused buffer for 400KB request, but reused count did not increase")
	}
	if len(buf) != 400*1024 {
		t.Fatalf("Expected len %d, got %d", 400*1024, len(buf))
	}
	if cap(buf) < 512*1024 {
		t.Fatalf("Expected cap at least 512KB, got %d", cap(buf))
	}
	pool.Put(buf)
}

func TestSyncBufferPoolBasic(t *testing.T) {
	pool := NewSyncBufferPool()

	testSizes := []int{100, 4096, 10000, 400 * 1024}
	for _, sz := range testSizes {
		buf := pool.Get(sz)
		if len(buf) != sz {
			t.Fatalf("Expected len %d, got %d", sz, len(buf))
		}
		if cap(buf) < sz {
			t.Fatalf("Expected cap >= %d, got %d", sz, cap(buf))
		}
		pool.Put(buf)
	}
}

func TestBufferPoolOptions(t *testing.T) {
	opts := GetDefaultOptions()
	if opts.BufferPool != nil || opts.BufferPoolSize != 0 {
		t.Fatalf("Default options should have nil BufferPool and 0 BufferPoolSize")
	}

	if err := WithBufferPool(64)(&opts); err != nil {
		t.Fatalf("WithBufferPool error: %v", err)
	}
	if opts.BufferPoolSize != 64 || opts.BufferPool == nil {
		t.Fatalf("Expected BufferPoolSize 64 and non-nil BufferPool")
	}

	customPool := NewSyncBufferPool()
	if err := WithCustomBufferPool(customPool)(&opts); err != nil {
		t.Fatalf("WithCustomBufferPool error: %v", err)
	}
	if opts.BufferPool != customPool {
		t.Fatalf("Expected custom buffer pool to be set")
	}
}

func TestBufferPoolMsgRelease(t *testing.T) {
	pool := NewTieredBufferPool(10)
	nc := &Conn{
		bufPool: pool,
		subs:    make(map[int64]*Subscription),
	}
	nc.ps = &parseState{}

	sub := &Subscription{
		conn: nc,
		sid:  1,
		mch:  make(chan *Msg, 10),
		typ:  ChanSubscription,
	}
	nc.subs[1] = sub

	// Simulate incoming message
	payload := []byte("hello world from buffer pool")
	proto := fmt.Sprintf("MSG test 1 %d\r\n%s\r\n", len(payload), string(payload))

	if err := nc.parse([]byte(proto)); err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	select {
	case m := <-sub.mch:
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Expected payload %q, got %q", payload, m.Data)
		}
		if m.rawBuf == nil {
			t.Fatalf("Expected rawBuf to be set on Msg")
		}
		origCap := cap(m.rawBuf)
		if origCap < 4096 {
			t.Fatalf("Expected rawBuf cap >= 4096, got %d", origCap)
		}

		// Release message
		m.Release()
		if m.Data != nil {
			t.Fatalf("Expected m.Data to be nil after Release, got %v", m.Data)
		}
		if m.rawBuf != nil {
			t.Fatalf("Expected m.rawBuf to be nil after Release")
		}

		// Double release should be a no-op and safe
		m.Release()

		// Verify stats reflect release
		stats := nc.Stats()
		if stats.ReleasedBufs != 1 {
			t.Fatalf("Expected ReleasedBufs=1, got %d", stats.ReleasedBufs)
		}

		// Next parse should reuse the released buffer
		if err := nc.parse([]byte(proto)); err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		m2 := <-sub.mch
		if !bytes.Equal(m2.Data, payload) {
			t.Fatalf("Expected payload %q, got %q", payload, m2.Data)
		}
		stats = nc.Stats()
		if stats.ReusedBufs != 1 {
			t.Fatalf("Expected ReusedBufs=1, got %d", stats.ReusedBufs)
		}
		m2.Release()
	default:
		t.Fatalf("Expected message on subscription channel")
	}
}

func TestBufferPoolWithHeaders(t *testing.T) {
	pool := NewTieredBufferPool(10)
	nc := &Conn{
		bufPool: pool,
		subs:    make(map[int64]*Subscription),
	}
	nc.ps = &parseState{}

	sub := &Subscription{
		conn: nc,
		sid:  1,
		mch:  make(chan *Msg, 10),
		typ:  ChanSubscription,
	}
	nc.subs[1] = sub

	hdr := "NATS/1.0\r\nHeader1: Value1\r\n\r\n"
	body := "payload with headers"
	totalLen := len(hdr) + len(body)
	proto := fmt.Sprintf("HMSG test 1 %d %d\r\n%s%s\r\n", len(hdr), totalLen, hdr, body)

	if err := nc.parse([]byte(proto)); err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	m := <-sub.mch
	if !bytes.Equal(m.Data, []byte(body)) {
		t.Fatalf("Expected body %q, got %q", body, string(m.Data))
	}
	if m.Header.Get("Header1") != "Value1" {
		t.Fatalf("Expected Header1=Value1, got %q", m.Header.Get("Header1"))
	}
	// Verify rawBuf has full capacity including the headers
	if cap(m.rawBuf) < 4096 {
		t.Fatalf("Expected cap(rawBuf) >= 4096, got %d", cap(m.rawBuf))
	}

	m.Release()
	if m.Data != nil {
		t.Fatalf("Expected m.Data to be nil after Release")
	}

	stats := nc.Stats()
	if stats.ReleasedBufs != 1 {
		t.Fatalf("Expected ReleasedBufs=1, got %d", stats.ReleasedBufs)
	}
}

func TestBufferPoolParserSplitMsg(t *testing.T) {
	pool := NewTieredBufferPool(10)
	nc := &Conn{
		bufPool: pool,
		subs:    make(map[int64]*Subscription),
	}
	nc.ps = &parseState{}

	sub := &Subscription{
		conn: nc,
		sid:  1,
		mch:  make(chan *Msg, 10),
		typ:  ChanSubscription,
	}
	nc.subs[1] = sub

	// Large payload larger than default scratch (4KB) and read buffer (32KB): e.g. 50 KB
	largePayload := make([]byte, 50*1024)
	for i := range largePayload {
		largePayload[i] = byte('A' + (i % 26))
	}

	header := fmt.Sprintf("MSG large.subj 1 %d\r\n", len(largePayload))
	full := append([]byte(header), largePayload...)
	full = append(full, []byte("\r\n")...)

	// Feed in chunks to simulate TCP socket reads (e.g. 16 KB chunks)
	chunkSize := 16 * 1024
	for offset := 0; offset < len(full); offset += chunkSize {
		end := offset + chunkSize
		if end > len(full) {
			end = len(full)
		}
		if err := nc.parse(full[offset:end]); err != nil {
			t.Fatalf("Parse error at offset %d: %v", offset, err)
		}
	}

	select {
	case m := <-sub.mch:
		if !bytes.Equal(m.Data, largePayload) {
			t.Fatalf("Received payload mismatch on large split message")
		}
		if m.rawBuf == nil {
			t.Fatalf("Expected rawBuf to be set on large message")
		}
		if cap(m.rawBuf) < 128*1024 { // 50KB should be placed in 128KB tier
			t.Fatalf("Expected rawBuf cap >= 128KB, got %d", cap(m.rawBuf))
		}

		m.Release()
		if m.Data != nil {
			t.Fatalf("Expected m.Data to be nil after Release")
		}
	default:
		t.Fatalf("Expected message to be delivered after chunks parsed")
	}

	// Next large message should reuse the buffer from the pool
	for offset := 0; offset < len(full); offset += chunkSize {
		end := offset + chunkSize
		if end > len(full) {
			end = len(full)
		}
		if err := nc.parse(full[offset:end]); err != nil {
			t.Fatalf("Parse error: %v", err)
		}
	}

	m2 := <-sub.mch
	if !bytes.Equal(m2.Data, largePayload) {
		t.Fatalf("Payload mismatch on second message")
	}
	m2.Release()

	stats := nc.Stats()
	if stats.ReusedBufs != 1 {
		t.Fatalf("Expected ReusedBufs=1 on split message, got %d", stats.ReusedBufs)
	}
	if stats.ReleasedBufs != 2 {
		t.Fatalf("Expected ReleasedBufs=2, got %d", stats.ReleasedBufs)
	}
}

func BenchmarkMessageProcessing(b *testing.B) {
	b.Run("WithoutPool", func(b *testing.B) {
		nc := &Conn{
			subs: make(map[int64]*Subscription),
		}
		nc.ps = &parseState{}
		sub := &Subscription{
			conn: nc,
			sid:  1,
			mch:  make(chan *Msg, 1000),
			typ:  ChanSubscription,
		}
		nc.subs[1] = sub

		payload := make([]byte, 400*1024)
		proto := fmt.Sprintf("MSG test 1 %d\r\n", len(payload))
		full := append([]byte(proto), payload...)
		full = append(full, []byte("\r\n")...)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Chunk into 32KB pieces
			for off := 0; off < len(full); off += 32768 {
				end := off + 32768
				if end > len(full) {
					end = len(full)
				}
				_ = nc.parse(full[off:end])
			}
			m := <-sub.mch
			_ = m.Data
		}
	})

	b.Run("WithTieredPool", func(b *testing.B) {
		pool := NewTieredBufferPool(10)
		nc := &Conn{
			bufPool: pool,
			subs:    make(map[int64]*Subscription),
		}
		nc.ps = &parseState{}
		sub := &Subscription{
			conn: nc,
			sid:  1,
			mch:  make(chan *Msg, 1000),
			typ:  ChanSubscription,
		}
		nc.subs[1] = sub

		payload := make([]byte, 400*1024)
		proto := fmt.Sprintf("MSG test 1 %d\r\n", len(payload))
		full := append([]byte(proto), payload...)
		full = append(full, []byte("\r\n")...)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Chunk into 32KB pieces
			for off := 0; off < len(full); off += 32768 {
				end := off + 32768
				if end > len(full) {
					end = len(full)
				}
				_ = nc.parse(full[off:end])
			}
			m := <-sub.mch
			_ = m.Data
			m.Release()
		}
	})
}
