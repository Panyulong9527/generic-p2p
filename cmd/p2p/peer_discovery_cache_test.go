package main

import (
	"testing"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/scheduler"
)

func TestPeerDiscoveryCacheReturnsStoredCandidatesBeforeExpiry(t *testing.T) {
	cache := newPeerDiscoveryCache(500 * time.Millisecond)
	now := time.Unix(100, 0)
	original := []scheduler.PeerCandidate{
		{
			PeerID:     "peer-a",
			HaveRanges: []core.HaveRange{{Start: 0, End: 2}},
		},
	}

	cache.Store("content-a", original, now)

	cached, ok := cache.Load("content-a", now.Add(200*time.Millisecond))
	if !ok {
		t.Fatal("expected cached candidates")
	}
	if len(cached) != 1 || cached[0].PeerID != "peer-a" {
		t.Fatalf("unexpected cached candidates: %+v", cached)
	}

	cached[0].HaveRanges[0].End = 99
	reloaded, ok := cache.Load("content-a", now.Add(250*time.Millisecond))
	if !ok {
		t.Fatal("expected cached candidates on reload")
	}
	if reloaded[0].HaveRanges[0].End != 2 {
		t.Fatalf("expected cached data to be cloned, got %+v", reloaded[0].HaveRanges)
	}
}

func TestPeerDiscoveryCacheExpiresAndSeparatesContent(t *testing.T) {
	cache := newPeerDiscoveryCache(300 * time.Millisecond)
	now := time.Unix(200, 0)

	cache.Store("content-a", []scheduler.PeerCandidate{{PeerID: "peer-a"}}, now)

	if _, ok := cache.Load("content-b", now.Add(100*time.Millisecond)); ok {
		t.Fatal("expected content IDs to be isolated")
	}
	if _, ok := cache.Load("content-a", now.Add(400*time.Millisecond)); ok {
		t.Fatal("expected cache entry to expire")
	}
}
