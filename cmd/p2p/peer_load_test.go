package main

import "testing"

func TestPeerLoadStateTracksPendingCounts(t *testing.T) {
	state := newPeerLoadState()

	if count := state.PendingCount("peer-a"); count != 0 {
		t.Fatalf("expected zero initial count, got %d", count)
	}

	state.Acquire("peer-a")
	state.Acquire("peer-a")
	if count := state.PendingCount("peer-a"); count != 2 {
		t.Fatalf("expected pending count 2, got %d", count)
	}

	state.Release("peer-a")
	if count := state.PendingCount("peer-a"); count != 1 {
		t.Fatalf("expected pending count 1 after release, got %d", count)
	}

	state.Release("peer-a")
	if count := state.PendingCount("peer-a"); count != 0 {
		t.Fatalf("expected pending count 0 after final release, got %d", count)
	}
}
