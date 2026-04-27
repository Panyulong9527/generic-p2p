package main

import (
	"testing"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/scheduler"
)

func TestPieceAttemptCandidatesPrefersSelectedAndTopUDPAlternatives(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:     "udp://selected",
		Transport:  "udp",
		Score:      1.6,
		HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:       "udp://alt-better",
			Transport:    "udp",
			Score:        1.5,
			PendingCount: 1,
			HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:       "udp://alt-best",
			Transport:    "udp",
			Score:        1.5,
			PendingCount: 0,
			HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "udp://missing-piece",
			Transport:  "udp",
			Score:      2.0,
			HaveRanges: []core.HaveRange{{Start: 8, End: 9}},
		},
		{
			PeerID:     "tcp://ignored",
			Transport:  "tcp",
			Score:      9.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	attempts := pieceAttemptCandidates(3, selected, peers)
	if len(attempts) != 3 {
		t.Fatalf("expected 3 attempt candidates, got %d", len(attempts))
	}
	if attempts[0].PeerID != "udp://selected" {
		t.Fatalf("expected selected peer first, got %s", attempts[0].PeerID)
	}
	if attempts[1].PeerID != "udp://alt-best" {
		t.Fatalf("expected best udp alternative second, got %s", attempts[1].PeerID)
	}
	if attempts[2].PeerID != "udp://alt-better" {
		t.Fatalf("expected next udp alternative third, got %s", attempts[2].PeerID)
	}
}

func TestPieceAttemptCandidatesReturnsOnlySelectedForTCP(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:     "tcp://selected",
		Transport:  "tcp",
		HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
	}

	attempts := pieceAttemptCandidates(1, selected, []scheduler.PeerCandidate{selected})
	if len(attempts) != 1 || attempts[0].PeerID != selected.PeerID {
		t.Fatalf("unexpected attempt candidates: %+v", attempts)
	}
}

func TestPieceAttemptCandidatesAllowsMoreAlternativesForAggressiveBurstProfile(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:       "udp://selected-aggressive",
		Transport:    "udp",
		BurstProfile: "aggressive",
		Score:        1.4,
		HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{PeerID: "udp://alt-1", Transport: "udp", Score: 1.3, HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
		{PeerID: "udp://alt-2", Transport: "udp", Score: 1.2, HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
		{PeerID: "udp://alt-3", Transport: "udp", Score: 1.1, HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
		{PeerID: "udp://alt-4", Transport: "udp", Score: 1.0, HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
	}

	attempts := pieceAttemptCandidates(2, selected, peers)
	if len(attempts) != 4 {
		t.Fatalf("expected 4 attempt candidates for aggressive burst profile, got %d", len(attempts))
	}
	if attempts[3].PeerID != "udp://alt-3" {
		t.Fatalf("expected third alternative to be included for aggressive profile, got %+v", attempts)
	}
}

func TestUDPAttemptBudgetVariesByBurstProfile(t *testing.T) {
	if got := udpAttemptBudget(scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm"}); got != 2 {
		t.Fatalf("expected warm budget 2, got %d", got)
	}
	if got := udpAttemptBudget(scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive"}); got != 4 {
		t.Fatalf("expected aggressive budget 4, got %d", got)
	}
	if got := udpAttemptBudget(scheduler.PeerCandidate{Transport: "udp"}); got != 3 {
		t.Fatalf("expected default budget 3, got %d", got)
	}
}

func TestUDPPieceTimeoutVariesByBurstProfile(t *testing.T) {
	if got := udpPieceTimeout(scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm"}); got != 3500*time.Millisecond {
		t.Fatalf("expected warm timeout 3.5s, got %s", got)
	}
	if got := udpPieceTimeout(scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive"}); got != 5500*time.Millisecond {
		t.Fatalf("expected aggressive timeout 5.5s, got %s", got)
	}
	if got := udpPieceTimeout(scheduler.PeerCandidate{Transport: "udp"}); got != 4500*time.Millisecond {
		t.Fatalf("expected default timeout 4.5s, got %s", got)
	}
}
