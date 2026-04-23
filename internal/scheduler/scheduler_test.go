package scheduler

import (
	"testing"

	"generic-p2p/internal/core"
)

func TestChoosePeerPrefersLAN(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:     "wan",
			IsLAN:      false,
			Score:      10,
			HaveRanges: []core.HaveRange{{Start: 0, End: 10}},
		},
		{
			PeerID:     "lan",
			IsLAN:      true,
			Score:      1,
			HaveRanges: []core.HaveRange{{Start: 0, End: 10}},
		},
	}

	peer, ok := s.ChoosePeer(5, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "lan" {
		t.Fatalf("expected lan peer, got %s", peer.PeerID)
	}
}

func TestChoosePeerSkipsPeerWithoutPiece(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:     "missing",
			IsLAN:      true,
			Score:      100,
			HaveRanges: []core.HaveRange{{Start: 0, End: 1}},
		},
		{
			PeerID:       "available",
			IsLAN:        false,
			Score:        5,
			PendingCount: 1,
			HaveRanges:   []core.HaveRange{{Start: 3, End: 6}},
		},
	}

	peer, ok := s.ChoosePeer(4, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "available" {
		t.Fatalf("expected available peer, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersLowerPendingCountWhenOtherwiseEqual(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:       "busy",
			IsLAN:        true,
			Score:        1,
			PendingCount: 2,
			HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:       "idle",
			IsLAN:        true,
			Score:        1,
			PendingCount: 0,
			HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(3, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "idle" {
		t.Fatalf("expected idle peer, got %s", peer.PeerID)
	}
}

func TestChoosePiecePrefersRarestAvailable(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:     "peer-a",
			HaveRanges: []core.HaveRange{{Start: 0, End: 4}},
		},
		{
			PeerID:     "peer-b",
			HaveRanges: []core.HaveRange{{Start: 0, End: 1}},
		},
		{
			PeerID:     "peer-c",
			HaveRanges: []core.HaveRange{{Start: 0, End: 0}},
		},
	}

	piece, ok := s.ChoosePiece(5, peers, map[int]bool{}, map[int]bool{})
	if !ok {
		t.Fatal("expected to choose a piece")
	}
	if piece != 2 {
		t.Fatalf("expected rarest available piece 2, got %d", piece)
	}
}

func TestChoosePieceSkipsCompletedAndInProgress(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:     "peer-a",
			HaveRanges: []core.HaveRange{{Start: 0, End: 3}},
		},
	}

	completed := map[int]bool{0: true}
	inProgress := map[int]bool{1: true}
	piece, ok := s.ChoosePiece(4, peers, completed, inProgress)
	if !ok {
		t.Fatal("expected to choose a piece")
	}
	if piece != 2 {
		t.Fatalf("expected piece 2, got %d", piece)
	}
}
