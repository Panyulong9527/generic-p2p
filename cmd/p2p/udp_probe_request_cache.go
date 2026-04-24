package main

import (
	"sync"
	"time"
)

type udpProbeRequestCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]time.Time
}

func newUDPProbeRequestCache(ttl time.Duration) *udpProbeRequestCache {
	return &udpProbeRequestCache{
		ttl:     ttl,
		entries: make(map[string]time.Time),
	}
}

func (c *udpProbeRequestCache) ShouldRequest(key string, now time.Time) bool {
	if c == nil {
		return true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt, ok := c.entries[key]
	if ok && now.Before(expiresAt) {
		return false
	}
	c.entries[key] = now.Add(c.ttl)
	return true
}
