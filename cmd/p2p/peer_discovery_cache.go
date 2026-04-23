package main

import (
	"sync"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/scheduler"
)

type peerDiscoveryCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	contentID  string
	expiresAt  time.Time
	candidates []scheduler.PeerCandidate
}

func newPeerDiscoveryCache(ttl time.Duration) *peerDiscoveryCache {
	return &peerDiscoveryCache{ttl: ttl}
}

func (c *peerDiscoveryCache) Load(contentID string, now time.Time) ([]scheduler.PeerCandidate, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.contentID != contentID || !now.Before(c.expiresAt) {
		return nil, false
	}
	return clonePeerCandidates(c.candidates), true
}

func (c *peerDiscoveryCache) Store(contentID string, candidates []scheduler.PeerCandidate, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.contentID = contentID
	c.expiresAt = now.Add(c.ttl)
	c.candidates = clonePeerCandidates(candidates)
}

func clonePeerCandidates(input []scheduler.PeerCandidate) []scheduler.PeerCandidate {
	if len(input) == 0 {
		return nil
	}

	out := make([]scheduler.PeerCandidate, len(input))
	for i, candidate := range input {
		out[i] = candidate
		if len(candidate.HaveRanges) > 0 {
			out[i].HaveRanges = append([]core.HaveRange(nil), candidate.HaveRanges...)
		}
	}
	return out
}
