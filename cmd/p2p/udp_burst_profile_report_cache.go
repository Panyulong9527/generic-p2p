package main

import "sync"

type udpBurstProfileReportCache struct {
	mu      sync.Mutex
	entries map[string]string
}

func newUDPBurstProfileReportCache() *udpBurstProfileReportCache {
	return &udpBurstProfileReportCache{
		entries: make(map[string]string),
	}
}

func (c *udpBurstProfileReportCache) ShouldReport(key string, fingerprint string) bool {
	if c == nil || key == "" || fingerprint == "" {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[key] == fingerprint {
		return false
	}
	c.entries[key] = fingerprint
	return true
}
