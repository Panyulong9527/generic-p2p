package main

import (
	"testing"

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
