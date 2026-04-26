package main

import (
	"testing"
	"time"
)

func TestPeerHealthStateAppliesCooldownAfterFailure(t *testing.T) {
	state := newPeerHealthState()
	now := time.Unix(100, 0)

	if !state.IsAvailable("peer-a", now) {
		t.Fatal("expected peer to start as available")
	}

	cooldown := state.MarkFailure("peer-a", now)
	if cooldown != 300*time.Millisecond {
		t.Fatalf("expected first cooldown to be 300ms, got %s", cooldown)
	}
	if state.IsAvailable("peer-a", now.Add(100*time.Millisecond)) {
		t.Fatal("expected peer to remain unavailable during cooldown")
	}
	if !state.IsAvailable("peer-a", now.Add(cooldown)) {
		t.Fatal("expected peer to become available after cooldown")
	}
}

func TestPeerHealthStateResetsOnSuccess(t *testing.T) {
	state := newPeerHealthState()
	now := time.Unix(200, 0)

	state.MarkFailure("peer-a", now)
	state.MarkFailure("peer-a", now.Add(time.Second))

	if remaining := state.RemainingCooldown("peer-a", now.Add(time.Second)); remaining == 0 {
		t.Fatal("expected cooldown after consecutive failures")
	}

	state.MarkSuccess("peer-a")

	if remaining := state.RemainingCooldown("peer-a", now.Add(time.Second)); remaining != 0 {
		t.Fatalf("expected cooldown to clear after success, got %s", remaining)
	}

	next := state.MarkFailure("peer-a", now.Add(2*time.Second))
	if next != 300*time.Millisecond {
		t.Fatalf("expected cooldown sequence to reset after success, got %s", next)
	}
}

func TestPeerCooldownForFailuresCapsBackoff(t *testing.T) {
	if got := peerCooldownForFailures(1); got != 300*time.Millisecond {
		t.Fatalf("expected 300ms, got %s", got)
	}
	if got := peerCooldownForFailures(3); got != 1200*time.Millisecond {
		t.Fatalf("expected 1200ms, got %s", got)
	}
	if got := peerCooldownForFailures(10); got != 5*time.Second {
		t.Fatalf("expected capped cooldown of 5s, got %s", got)
	}
}

func TestPeerCooldownForUDPTimeoutFailuresUsesShorterBackoff(t *testing.T) {
	if got := peerCooldownForFailureKind(1, peerFailureKindUDPTimeout); got != 120*time.Millisecond {
		t.Fatalf("expected 120ms, got %s", got)
	}
	if got := peerCooldownForFailureKind(3, peerFailureKindUDPTimeout); got != 480*time.Millisecond {
		t.Fatalf("expected 480ms, got %s", got)
	}
	if got := peerCooldownForFailureKind(10, peerFailureKindUDPTimeout); got != 1500*time.Millisecond {
		t.Fatalf("expected capped cooldown of 1500ms, got %s", got)
	}
}
