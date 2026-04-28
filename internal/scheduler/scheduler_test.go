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

func TestChoosePeerPrefersTCPOverLowValueUDPWhenScoresAreClose(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:          "udp-low",
			Transport:       "udp",
			Score:           1.26,
			UDPDecisionRisk: "low",
			HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "tcp-steady" {
		t.Fatalf("expected tcp peer to win over low-value udp, got %s", peer.PeerID)
	}
}

func TestChoosePeerStillAllowsLowValueUDPWhenScoreIsClearlyHigher(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:          "udp-low",
			Transport:       "udp",
			Score:           1.5,
			UDPDecisionRisk: "low",
			HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "udp-low" {
		t.Fatalf("expected udp peer to win when score gap is large, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersTCPOverWatchUDPWhenScoresAreNear(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:          "udp-watch",
			Transport:       "udp",
			Score:           1.12,
			UDPDecisionRisk: "warn",
			HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "tcp-steady" {
		t.Fatalf("expected tcp peer to win over watch udp, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersPublicMappedUDPWhenScoresAreClose(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:          "udp-public",
			Transport:       "udp",
			Score:           1.02,
			UDPPublicMapped: true,
			HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "udp-public" {
		t.Fatalf("expected public-mapped udp to win close score race, got %s", peer.PeerID)
	}
}

func TestChoosePeerDoesNotPreferPublicMappedUDPWhenRiskIsSuppressed(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:          "udp-public-low",
			Transport:       "udp",
			Score:           1.08,
			UDPPublicMapped: true,
			UDPDecisionRisk: "low",
			HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "tcp-steady" {
		t.Fatalf("expected tcp peer to keep winning when public-mapped udp is low risk, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersHealthyUDPChunkProgressWhenScoresAreClose(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:               "udp-healthy",
			Transport:            "udp",
			Score:                1.08,
			UDPChunkSamples:      3,
			UDPChunkReceiveRatio: 0.95,
			UDPChunkCompleteRate: 0.8,
			HaveRanges:           []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "udp-healthy" {
		t.Fatalf("expected healthy udp chunk path to win close score race, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersBulkTopologyRoleWhenScoresAreClose(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:           "udp-bulk",
			Transport:        "udp",
			Score:            1.06,
			PeerTopologyRole: "bulk",
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:           "tcp-backup",
			Transport:        "tcp",
			Score:            1.0,
			PeerTopologyRole: "backup",
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "udp-bulk" {
		t.Fatalf("expected bulk topology peer, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersBackupOverFallbackWhenScoresAreClose(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:           "udp-fallback",
			Transport:        "udp",
			Score:            1.12,
			PeerTopologyRole: "fallback",
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:           "tcp-backup",
			Transport:        "tcp",
			Score:            1.0,
			PeerTopologyRole: "backup",
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "tcp-backup" {
		t.Fatalf("expected backup peer to beat fallback path, got %s", peer.PeerID)
	}
}

func TestChoosePeerPrefersTCPOverWeakUDPChunkProgressWhenScoresAreClose(t *testing.T) {
	s := Scheduler{}
	peers := []PeerCandidate{
		{
			PeerID:               "udp-weak",
			Transport:            "udp",
			Score:                1.12,
			UDPChunkSamples:      3,
			UDPChunkReceiveRatio: 0.25,
			UDPChunkCompleteRate: 0.1,
			HaveRanges:           []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:     "tcp-steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	peer, ok := s.ChoosePeer(2, peers)
	if !ok {
		t.Fatal("expected to select a peer")
	}
	if peer.PeerID != "tcp-steady" {
		t.Fatalf("expected tcp peer to win over weak udp chunk path, got %s", peer.PeerID)
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
