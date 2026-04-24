package main

import (
	"testing"
	"time"
)

func TestUDPProbeRequestCacheSuppressesRepeatedRequests(t *testing.T) {
	cache := newUDPProbeRequestCache(500 * time.Millisecond)
	now := time.Unix(100, 0)

	if !cache.ShouldRequest("content-a|peer-a", now) {
		t.Fatal("expected first request to be allowed")
	}
	if cache.ShouldRequest("content-a|peer-a", now.Add(200*time.Millisecond)) {
		t.Fatal("expected repeated request to be suppressed")
	}
	if !cache.ShouldRequest("content-a|peer-a", now.Add(600*time.Millisecond)) {
		t.Fatal("expected request after expiry to be allowed")
	}
}
