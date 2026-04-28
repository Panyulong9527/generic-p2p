package main

import "sync"

type udpDecisionReportCache struct {
	mu      sync.Mutex
	entries map[string]string
}

func newUDPDecisionReportCache() *udpDecisionReportCache {
	return &udpDecisionReportCache{
		entries: make(map[string]string),
	}
}

func (c *udpDecisionReportCache) ShouldReport(key string, fingerprint string) bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[key]; ok && existing == fingerprint {
		return false
	}
	c.entries[key] = fingerprint
	return true
}
