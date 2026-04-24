package main

import (
	"testing"
	"time"
)

func TestUDPProbeCacheReturnsStoredResultBeforeExpiry(t *testing.T) {
	cache := newUDPProbeCache(500 * time.Millisecond)
	now := time.Unix(100, 0)

	cache.Store("127.0.0.1:9000", true, now)

	ok, cached := cache.Load("127.0.0.1:9000", now.Add(200*time.Millisecond))
	if !cached {
		t.Fatal("expected cached probe result")
	}
	if !ok {
		t.Fatal("expected cached success")
	}
}

func TestUDPProbeCacheStoresFailuresAndExpires(t *testing.T) {
	cache := newUDPProbeCache(300 * time.Millisecond)
	now := time.Unix(200, 0)

	cache.Store("127.0.0.1:9001", false, now)

	ok, cached := cache.Load("127.0.0.1:9001", now.Add(100*time.Millisecond))
	if !cached {
		t.Fatal("expected cached failure")
	}
	if ok {
		t.Fatal("expected cached failure result")
	}

	if _, cached := cache.Load("127.0.0.1:9001", now.Add(400*time.Millisecond)); cached {
		t.Fatal("expected cache entry to expire")
	}
}
