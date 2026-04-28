package main

import (
	"testing"
	"time"

	"generic-p2p/internal/core"
	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/scheduler"
)

func TestAnnotatePeerTopologyMarksActiveHealthyUDPAsBulk(t *testing.T) {
	now := time.Now()
	contentID := "sha256-topology-bulk"
	peerID := "udp://bulk-peer"
	noteUDPSessionStageSuccess(peerID, "198.51.100.91:9003", "probe", contentID, now.Add(-12*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.91:9003", "have", contentID, now.Add(-8*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.91:9003", "piece", contentID, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, peerID, p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        720 * time.Millisecond,
		Completed:       true,
	}, now.Add(-2*time.Second))
	recordUDPChunkProgress(contentID, peerID, p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        760 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))

	candidates := annotatePeerTopology([]scheduler.PeerCandidate{
		{
			PeerID:               peerID,
			Transport:            "udp",
			Score:                1.45,
			UDPPublicMapped:      true,
			UDPChunkSamples:      2,
			UDPChunkReceiveRatio: 0.95,
			UDPChunkCompleteRate: 0.80,
			HaveRanges:           []core.HaveRange{{Start: 0, End: 4}},
		},
	}, contentID, now)

	if candidates[0].PeerTopologyRole != peerTopologyRoleBulk {
		t.Fatalf("expected bulk topology role, got %s", candidates[0].PeerTopologyRole)
	}
	if candidates[0].PathAssistScore <= candidates[0].Score {
		t.Fatalf("expected assist score to be boosted above raw score, got %.2f <= %.2f", candidates[0].PathAssistScore, candidates[0].Score)
	}
}

func TestAnnotatePeerTopologyMarksSuppressedUDPAsFallback(t *testing.T) {
	now := time.Now()
	candidates := annotatePeerTopology([]scheduler.PeerCandidate{
		{
			PeerID:          "udp://fallback-peer",
			Transport:       "udp",
			Score:           1.18,
			UDPDecisionRisk: "low",
			HaveRanges:      []core.HaveRange{{Start: 0, End: 4}},
		},
	}, "sha256-topology-fallback", now)

	if candidates[0].PeerTopologyRole != peerTopologyRoleFallback {
		t.Fatalf("expected fallback topology role, got %s", candidates[0].PeerTopologyRole)
	}
}

func TestAnnotatePeerTopologyUsesStickyBulkAffinity(t *testing.T) {
	now := time.Now()
	contentID := "sha256-topology-sticky"
	peerID := "udp://sticky-peer"
	noteUDPSessionStageSuccess(peerID, "198.51.100.92:9003", "piece", contentID, now.Add(-2*time.Second))

	candidates := annotatePeerTopology([]scheduler.PeerCandidate{
		{
			PeerID:     peerID,
			Transport:  "udp",
			Score:      1.12,
			HaveRanges: []core.HaveRange{{Start: 0, End: 4}},
		},
	}, contentID, now)

	if candidates[0].PeerTopologyRole != peerTopologyRoleBulk {
		t.Fatalf("expected sticky bulk role, got %s", candidates[0].PeerTopologyRole)
	}
}

func TestAnnotatePeerTopologyUsesQuarantineFallback(t *testing.T) {
	now := time.Now()
	contentID := "sha256-topology-quarantine"
	peerID := "udp://quarantine-peer"
	noteUDPSessionStageSuccess(peerID, "198.51.100.93:9003", "piece", contentID, now.Add(-20*time.Second))
	noteUDPSessionStageFailure(peerID, "198.51.100.93:9003", "piece", "udp_timeout", now.Add(-4*time.Second))
	noteUDPSessionStageFailure(peerID, "198.51.100.93:9003", "piece", "udp_timeout", now.Add(-1*time.Second))

	candidates := annotatePeerTopology([]scheduler.PeerCandidate{
		{
			PeerID:     peerID,
			Transport:  "udp",
			Score:      1.20,
			HaveRanges: []core.HaveRange{{Start: 0, End: 4}},
		},
	}, contentID, now)

	if candidates[0].PeerTopologyRole != peerTopologyRoleFallback {
		t.Fatalf("expected quarantine fallback role, got %s", candidates[0].PeerTopologyRole)
	}
}
