package main

import (
	"sync"
	"time"
)

type udpProbeCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]udpProbeCacheEntry
}

type udpProbeCacheEntry struct {
	ok        bool
	expiresAt time.Time
}

func newUDPProbeCache(ttl time.Duration) *udpProbeCache {
	return &udpProbeCache{
		ttl:     ttl,
		entries: make(map[string]udpProbeCacheEntry),
	}
}

func (c *udpProbeCache) Load(addr string, now time.Time) (bool, bool) {
	if c == nil {
		return false, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[addr]
	if !ok {
		return false, false
	}
	if !now.Before(entry.expiresAt) {
		delete(c.entries, addr)
		return false, false
	}
	return entry.ok, true
}

func (c *udpProbeCache) Store(addr string, ok bool, now time.Time) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[addr] = udpProbeCacheEntry{
		ok:        ok,
		expiresAt: now.Add(c.ttl),
	}
}
