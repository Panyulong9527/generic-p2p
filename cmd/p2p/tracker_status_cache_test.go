package main

import (
	"testing"
	"time"

	"generic-p2p/internal/tracker"
)

func TestTrackerStatusCacheReturnsStoredStatusBeforeExpiry(t *testing.T) {
	cache := newTrackerStatusCache(500 * time.Millisecond)
	now := time.Unix(100, 0)
	status := tracker.StatusResponse{PeerCount: 3}

	cache.Store("http://tracker.test", status, now)

	cached, ok := cache.Load("http://tracker.test", now.Add(200*time.Millisecond))
	if !ok {
		t.Fatal("expected cached status")
	}
	if cached.PeerCount != 3 {
		t.Fatalf("unexpected cached peer count: %d", cached.PeerCount)
	}
}

func TestTrackerStatusCacheExpiresAndSeparatesTrackerURL(t *testing.T) {
	cache := newTrackerStatusCache(300 * time.Millisecond)
	now := time.Unix(200, 0)

	cache.Store("http://tracker-a.test", tracker.StatusResponse{PeerCount: 1}, now)

	if _, ok := cache.Load("http://tracker-b.test", now.Add(100*time.Millisecond)); ok {
		t.Fatal("expected tracker URLs to be isolated")
	}
	if _, ok := cache.Load("http://tracker-a.test", now.Add(400*time.Millisecond)); ok {
		t.Fatal("expected cache entry to expire")
	}
}
