package main

import (
	"sync"
	"time"

	"generic-p2p/internal/tracker"
)

type trackerStatusCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	trackerURL string
	expiresAt  time.Time
	status     tracker.StatusResponse
}

func newTrackerStatusCache(ttl time.Duration) *trackerStatusCache {
	return &trackerStatusCache{ttl: ttl}
}

func (c *trackerStatusCache) Load(trackerURL string, now time.Time) (tracker.StatusResponse, bool) {
	if c == nil {
		return tracker.StatusResponse{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.trackerURL != trackerURL || !now.Before(c.expiresAt) {
		return tracker.StatusResponse{}, false
	}
	return c.status, true
}

func (c *trackerStatusCache) Store(trackerURL string, status tracker.StatusResponse, now time.Time) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.trackerURL = trackerURL
	c.expiresAt = now.Add(c.ttl)
	c.status = status
}
