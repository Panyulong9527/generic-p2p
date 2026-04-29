package main

import (
	"path/filepath"
	"testing"
	"time"

	"generic-p2p/internal/core"
	p2pnet "generic-p2p/internal/net"
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

	attempts := pieceAttemptCandidates("", 3, selected, peers)
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

	attempts := pieceAttemptCandidates("", 1, selected, []scheduler.PeerCandidate{selected})
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

	attempts := pieceAttemptCandidates("", 2, selected, peers)
	if len(attempts) != 4 {
		t.Fatalf("expected 4 attempt candidates for aggressive burst profile, got %d", len(attempts))
	}
	if attempts[3].PeerID != "udp://alt-3" {
		t.Fatalf("expected third alternative to be included for aggressive profile, got %+v", attempts)
	}
}

func TestPieceAttemptCandidatesPrefersLowerRiskUDPAlternativesOnEqualScore(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:       "udp://selected-risk",
		Transport:    "udp",
		BurstProfile: "default",
		Score:        1.4,
		HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{PeerID: "udp://alt-low", Transport: "udp", Score: 1.3, UDPDecisionRisk: "low", HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
		{PeerID: "udp://alt-stable", Transport: "udp", Score: 1.3, UDPDecisionRisk: "stable", HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
		{PeerID: "udp://alt-warn", Transport: "udp", Score: 1.3, UDPDecisionRisk: "warn", HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
	}

	attempts := pieceAttemptCandidates("", 2, selected, peers)
	if len(attempts) != 3 {
		t.Fatalf("expected selected plus two alternatives, got %+v", attempts)
	}
	if attempts[1].PeerID != "udp://alt-stable" || attempts[2].PeerID != "udp://alt-warn" {
		t.Fatalf("expected lower risk alternatives first, got %+v", attempts)
	}
}

func TestPieceAttemptCandidatesPrefersBetterChunkProgressOnEqualScore(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-alt-progress"
	selected := scheduler.PeerCandidate{
		PeerID:       "udp://selected-progress",
		Transport:    "udp",
		BurstProfile: "aggressive",
		Score:        1.4,
		HaveRanges:   []core.HaveRange{{Start: 0, End: 5}},
	}
	recordUDPChunkProgress(contentID, "udp://alt-fast", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        700 * time.Millisecond,
		Completed:       true,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://alt-fast", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        760 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))
	recordUDPChunkProgress(contentID, "udp://alt-slow", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1500 * time.Millisecond,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://alt-slow", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1450 * time.Millisecond,
	}, now.Add(-1*time.Second))

	peers := []scheduler.PeerCandidate{
		selected,
		{PeerID: "udp://alt-slow", Transport: "udp", Score: 1.3, HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
		{PeerID: "udp://alt-fast", Transport: "udp", Score: 1.3, HaveRanges: []core.HaveRange{{Start: 0, End: 5}}},
	}

	attempts := pieceAttemptCandidates(contentID, 2, selected, peers)
	if len(attempts) < 3 {
		t.Fatalf("expected selected plus two alternatives, got %+v", attempts)
	}
	if attempts[1].PeerID != "udp://alt-fast" {
		t.Fatalf("expected better chunk-progress alternative first, got %+v", attempts)
	}
}

func TestPieceAttemptCandidatesPrefersStickyBulkAlternative(t *testing.T) {
	now := time.Now()
	contentID := "sha256-attempt-sticky-bulk"
	noteUDPSessionStageSuccess("udp://alt-sticky", "198.51.100.94:9003", "piece", contentID, now.Add(-2*time.Second))

	selected := scheduler.PeerCandidate{
		PeerID:           "udp://selected",
		Transport:        "udp",
		Score:            1.40,
		PeerTopologyRole: peerTopologyRoleAssist,
		HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:           "udp://alt-plain",
			Transport:        "udp",
			Score:            1.40,
			PeerTopologyRole: peerTopologyRoleAssist,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:           "udp://alt-sticky",
			Transport:        "udp",
			Score:            1.40,
			PeerTopologyRole: peerTopologyRoleAssist,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	attempts := pieceAttemptCandidates(contentID, 2, selected, peers)
	if len(attempts) < 3 || attempts[1].PeerID != "udp://alt-sticky" {
		t.Fatalf("expected sticky bulk alternative first, got %+v", attempts)
	}
}

func TestPieceAttemptCandidatesSkipsQuarantinedAlternative(t *testing.T) {
	now := time.Now()
	contentID := "sha256-attempt-quarantine"
	noteUDPSessionStageSuccess("udp://alt-bad", "198.51.100.95:9003", "piece", contentID, now.Add(-20*time.Second))
	noteUDPSessionStageFailure("udp://alt-bad", "198.51.100.95:9003", "piece", "udp_timeout", now.Add(-4*time.Second))
	noteUDPSessionStageFailure("udp://alt-bad", "198.51.100.95:9003", "piece", "udp_timeout", now.Add(-1*time.Second))

	selected := scheduler.PeerCandidate{
		PeerID:           "udp://selected",
		Transport:        "udp",
		Score:            1.40,
		PeerTopologyRole: peerTopologyRoleBulk,
		HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:           "udp://alt-good",
			Transport:        "udp",
			Score:            1.38,
			PeerTopologyRole: peerTopologyRoleAssist,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
		{
			PeerID:           "udp://alt-bad",
			Transport:        "udp",
			Score:            1.50,
			PeerTopologyRole: peerTopologyRoleAssist,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	attempts := pieceAttemptCandidates(contentID, 2, selected, peers)
	for _, item := range attempts {
		if item.PeerID == "udp://alt-bad" {
			t.Fatalf("expected quarantined alternative to be skipped, got %+v", attempts)
		}
	}
}

func TestUDPAttemptBudgetVariesByBurstProfile(t *testing.T) {
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm"}); got != 2 {
		t.Fatalf("expected warm budget 2, got %d", got)
	}
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive"}); got != 4 {
		t.Fatalf("expected aggressive budget 4, got %d", got)
	}
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp"}); got != 3 {
		t.Fatalf("expected default budget 3, got %d", got)
	}
}

func TestUDPAttemptBudgetAdjustsByLastStage(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-budget-stage"
	recordUDPBurstOutcome(contentID, "udp://probe-peer", "aggressive", "probe", false, now.Add(-2*time.Second))
	recordUDPBurstOutcome(contentID, "udp://piece-peer", "default", "piece", false, now.Add(-2*time.Second))

	if got := udpAttemptBudget(contentID, scheduler.PeerCandidate{Transport: "udp", PeerID: "udp://probe-peer", BurstProfile: "aggressive"}); got != 3 {
		t.Fatalf("expected probe-stage budget to cool down to 3, got %d", got)
	}
	if got := udpAttemptBudget(contentID, scheduler.PeerCandidate{Transport: "udp", PeerID: "udp://piece-peer", BurstProfile: "default"}); got != 4 {
		t.Fatalf("expected piece-stage budget to expand to 4, got %d", got)
	}
}

func TestUDPAttemptBudgetAdjustsByDecisionRisk(t *testing.T) {
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive", UDPDecisionRisk: "low"}); got != 2 {
		t.Fatalf("expected low-risk penalty to shrink aggressive budget to 2, got %d", got)
	}
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "default", UDPDecisionRisk: "warn"}); got != 2 {
		t.Fatalf("expected watch-risk penalty to shrink default budget to 2, got %d", got)
	}
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm", UDPDecisionRisk: "stable"}); got != 3 {
		t.Fatalf("expected stable risk to widen warm budget to 3, got %d", got)
	}
}

func TestUDPAttemptBudgetAdjustsBySmoothedChunkProgress(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-budget-progress"

	recordUDPChunkProgress(contentID, "udp://budget-fast-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        700 * time.Millisecond,
		Completed:       true,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://budget-fast-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        720 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))

	recordUDPChunkProgress(contentID, "udp://budget-weak-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1500 * time.Millisecond,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://budget-weak-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1400 * time.Millisecond,
	}, now.Add(-1*time.Second))

	if got := udpAttemptBudget(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://budget-fast-peer",
	}); got != 4 {
		t.Fatalf("expected smoothed strong progress to widen default budget to 4, got %d", got)
	}

	if got := udpAttemptBudget(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://budget-weak-peer",
	}); got != 2 {
		t.Fatalf("expected smoothed weak progress to shrink default budget to 2, got %d", got)
	}
}

func TestUDPAttemptBudgetAdjustsBySessionState(t *testing.T) {
	now := time.Now()
	activePeer := "udp://session-budget-active"
	coolingPeer := "udp://session-budget-cooling"

	noteUDPSessionSuccess(activePeer, "198.51.100.81:9003", "sha256-session-budget", now.Add(-6*time.Second))
	noteUDPSessionSuccess(coolingPeer, "198.51.100.82:9003", "sha256-session-budget", now.Add(-35*time.Second))
	noteUDPSessionFailure(coolingPeer, "198.51.100.82:9003", now.Add(-3*time.Second))

	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", PeerID: activePeer}); got != 4 {
		t.Fatalf("expected active session to widen default budget to 4, got %d", got)
	}
	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", PeerID: coolingPeer}); got != 2 {
		t.Fatalf("expected cooling session to shrink default budget to 2, got %d", got)
	}
}

func TestUDPAttemptBudgetUsesStrongSessionRecommendation(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-budget-strong"

	noteUDPSessionStageSuccess(peerID, "198.51.100.83:9003", "probe", "sha256-session-budget", now.Add(-12*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.83:9003", "have", "sha256-session-budget", now.Add(-8*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.83:9003", "piece", "sha256-session-budget", now.Add(-4*time.Second))

	if got := udpAttemptBudget("", scheduler.PeerCandidate{Transport: "udp", PeerID: peerID}); got != 5 {
		t.Fatalf("expected strong active session to widen default budget to 5, got %d", got)
	}
}

func TestUDPPieceTimeoutVariesByBurstProfile(t *testing.T) {
	if got := udpPieceTimeout("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm"}); got != 3500*time.Millisecond {
		t.Fatalf("expected warm timeout 3.5s, got %s", got)
	}
	if got := udpPieceTimeout("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive"}); got != 5500*time.Millisecond {
		t.Fatalf("expected aggressive timeout 5.5s, got %s", got)
	}
	if got := udpPieceTimeout("", scheduler.PeerCandidate{Transport: "udp"}); got != 4500*time.Millisecond {
		t.Fatalf("expected default timeout 4.5s, got %s", got)
	}
}

func TestUDPPieceTimeoutAdjustsByLastStage(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-timeout-stage"
	recordUDPBurstOutcome(contentID, "udp://probe-timeout-peer", "aggressive", "probe", false, now.Add(-2*time.Second))
	recordUDPBurstOutcome(contentID, "udp://piece-timeout-peer", "default", "piece", false, now.Add(-2*time.Second))

	if got := udpPieceTimeout(contentID, scheduler.PeerCandidate{Transport: "udp", PeerID: "udp://probe-timeout-peer", BurstProfile: "aggressive"}); got != 4600*time.Millisecond {
		t.Fatalf("expected probe-stage timeout to tighten to 4.6s, got %s", got)
	}
	if got := udpPieceTimeout(contentID, scheduler.PeerCandidate{Transport: "udp", PeerID: "udp://piece-timeout-peer", BurstProfile: "default"}); got != 5700*time.Millisecond {
		t.Fatalf("expected piece-stage timeout to widen to 5.7s, got %s", got)
	}
}

func TestUDPPieceTimeoutAdjustsByDecisionRisk(t *testing.T) {
	if got := udpPieceTimeout("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive", UDPDecisionRisk: "low"}); got != 4300*time.Millisecond {
		t.Fatalf("expected low-risk penalty to tighten aggressive timeout to 4.3s, got %s", got)
	}
	if got := udpPieceTimeout("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "default", UDPDecisionRisk: "warn"}); got != 3900*time.Millisecond {
		t.Fatalf("expected watch-risk penalty to tighten default timeout to 3.9s, got %s", got)
	}
	if got := udpPieceTimeout("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm", UDPDecisionRisk: "stable"}); got != 4400*time.Millisecond {
		t.Fatalf("expected stable risk to widen warm timeout to 4.4s, got %s", got)
	}
}

func TestSelectionReasonMarksPublicMappedUDPCloseScorePreference(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:          "udp://public",
		Transport:       "udp",
		Score:           1.04,
		UDPPublicMapped: true,
		HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:     "tcp://steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_udp_public_mapped_close_score" {
		t.Fatalf("expected public-mapped close-score reason, got %s", got)
	}
}

func TestSelectionReasonSkipsPublicMappedReasonForSuppressedRisk(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:          "udp://public-low",
		Transport:       "udp",
		Score:           1.08,
		UDPPublicMapped: true,
		UDPDecisionRisk: "low",
		HaveRanges:      []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:     "tcp://steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_udp_despite_low_value_risk" {
		t.Fatalf("expected low-risk reason to win, got %s", got)
	}
}

func TestSelectionReasonMarksChunkProgressCloseScorePreference(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:               "udp://healthy",
		Transport:            "udp",
		Score:                1.08,
		UDPChunkSamples:      3,
		UDPChunkReceiveRatio: 0.92,
		UDPChunkCompleteRate: 0.8,
		HaveRanges:           []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:     "tcp://steady",
			Transport:  "tcp",
			Score:      1.0,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_udp_chunk_progress_close_score" {
		t.Fatalf("expected chunk-progress close-score reason, got %s", got)
	}
}

func TestSelectionReasonMarksBulkTopologyPreference(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:           "udp://bulk",
		Transport:        "udp",
		Score:            1.06,
		PeerTopologyRole: peerTopologyRoleBulk,
		HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:           "tcp://backup",
			Transport:        "tcp",
			Score:            1.0,
			PeerTopologyRole: peerTopologyRoleBackup,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_udp_bulk_path" {
		t.Fatalf("expected bulk topology reason, got %s", got)
	}
}

func TestPreferUDPContentTakeoverSelectionPrefersOwnerOverCloseTCP(t *testing.T) {
	now := time.Now()
	contentID := "sha256-download-takeover-owner"
	handoffPeer := "udp://handoff-source"
	takeoverPeer := "udp://takeover-owner"

	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.111:9003", contentID, 1, true, now.Add(-6*time.Second))
	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.111:9003", contentID, 1, true, now.Add(-3*time.Second))
	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.111:9003", contentID, 1, false, now)
	setUDPContentRouteTakeoverOwner(contentID, takeoverPeer, now)

	selected := scheduler.PeerCandidate{
		PeerID:     "tcp://steady",
		Transport:  "tcp",
		Score:      1.00,
		HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:           takeoverPeer,
			Transport:        "udp",
			Score:            0.92,
			PeerTopologyRole: peerTopologyRoleBulk,
			PathAssistScore:  1.45,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	got := preferUDPContentTakeoverSelection(contentID, 2, selected, peers, now)
	if got.PeerID != takeoverPeer {
		t.Fatalf("expected takeover owner to override close tcp selection, got %+v", got)
	}
}

func TestSelectionReasonMarksTakeoverOwnerPreference(t *testing.T) {
	now := time.Now()
	contentID := "sha256-download-takeover-reason"
	handoffPeer := "udp://handoff-reason"
	takeoverPeer := "udp://takeover-reason"

	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.121:9003", contentID, 1, true, now.Add(-6*time.Second))
	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.121:9003", contentID, 1, true, now.Add(-3*time.Second))
	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.121:9003", contentID, 1, false, now)
	noteUDPSessionStageSuccess(takeoverPeer, "198.51.100.122:9003", "piece", contentID, now.Add(-2*time.Second))
	setUDPContentRouteTakeoverOwner(contentID, takeoverPeer, now)

	selected := scheduler.PeerCandidate{
		PeerID:           takeoverPeer,
		Transport:        "udp",
		Score:            0.96,
		PeerTopologyRole: peerTopologyRoleBulk,
		PathAssistScore:  1.42,
		HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:     "tcp://steady",
			Transport:  "tcp",
			Score:      1.02,
			HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_udp_takeover_owner" {
		t.Fatalf("expected takeover owner reason, got %s", got)
	}
}

func TestSelectionReasonMarksTCPOverFallbackUDP(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:           "tcp://backup",
		Transport:        "tcp",
		Score:            1.0,
		PeerTopologyRole: peerTopologyRoleBackup,
		HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:           "udp://fallback",
			Transport:        "udp",
			Score:            1.20,
			PeerTopologyRole: peerTopologyRoleFallback,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_tcp_over_fallback_udp" {
		t.Fatalf("expected tcp over fallback udp reason, got %s", got)
	}
}

func TestSelectionReasonMarksTCPOverWeakUDPChunkProgress(t *testing.T) {
	selected := scheduler.PeerCandidate{
		PeerID:     "tcp://steady",
		Transport:  "tcp",
		Score:      1.0,
		HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:               "udp://weak",
			Transport:            "udp",
			Score:                1.12,
			UDPChunkSamples:      3,
			UDPChunkReceiveRatio: 0.25,
			UDPChunkCompleteRate: 0.1,
			HaveRanges:           []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	if got := selectionReason(2, selected, peers); got != "selected_tcp_over_weak_udp_progress" {
		t.Fatalf("expected weak-udp-progress tcp reason, got %s", got)
	}
}

func TestUDPPieceChunkWindowVariesByProfile(t *testing.T) {
	if got := udpPieceChunkWindowForCandidate("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm"}); got != 5 {
		t.Fatalf("expected warm chunk window 5, got %d", got)
	}
	if got := udpPieceChunkWindowForCandidate("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive"}); got != 3 {
		t.Fatalf("expected aggressive chunk window 3, got %d", got)
	}
	if got := udpPieceChunkWindowForCandidate("", scheduler.PeerCandidate{Transport: "udp"}); got != 4 {
		t.Fatalf("expected default chunk window 4, got %d", got)
	}
}

func TestReserveNextPiecePrefersTakeoverOwnerContinuity(t *testing.T) {
	now := time.Now()
	contentID := "sha256-reserve-takeover"
	setUDPContentRouteTakeoverOwner(contentID, "udp://takeover-owner", now)

	manifest := &core.ContentManifest{
		Version:   1,
		ContentID: contentID,
		TotalSize: 12,
		PieceSize: 4,
		Pieces: []core.PieceInfo{
			{Index: 0, Length: 4, Hash: core.HashBytes([]byte("p000"))},
			{Index: 1, Length: 4, Hash: core.HashBytes([]byte("p111"))},
			{Index: 2, Length: 4, Hash: core.HashBytes([]byte("p222"))},
		},
	}
	store, err := core.OpenPieceStore(filepath.Join(t.TempDir(), "store"), manifest)
	if err != nil {
		t.Fatalf("open piece store: %v", err)
	}
	state := &downloadPieceState{inProgress: map[int]bool{}}
	peers := []scheduler.PeerCandidate{
		{
			PeerID:     "udp://takeover-owner",
			Transport:  "udp",
			HaveRanges: []core.HaveRange{{Start: 1, End: 2}},
		},
		{
			PeerID:     "udp://other",
			Transport:  "udp",
			HaveRanges: []core.HaveRange{{Start: 0, End: 2}},
		},
	}

	pieceIndex, ok := reserveNextPiece(manifest, store, state, peers)
	if !ok {
		t.Fatal("expected piece reservation")
	}
	if pieceIndex != 1 {
		t.Fatalf("expected takeover-capable rarest piece 1, got %d", pieceIndex)
	}
}

func TestReserveNextPieceFallsBackWhenTakeoverOwnerLacksPiece(t *testing.T) {
	now := time.Now()
	contentID := "sha256-reserve-fallback"
	setUDPContentRouteTakeoverOwner(contentID, "udp://takeover-owner", now)

	manifest := &core.ContentManifest{
		Version:   1,
		ContentID: contentID,
		TotalSize: 8,
		PieceSize: 4,
		Pieces: []core.PieceInfo{
			{Index: 0, Length: 4, Hash: core.HashBytes([]byte("p000"))},
			{Index: 1, Length: 4, Hash: core.HashBytes([]byte("done"))},
		},
	}
	store, err := core.OpenPieceStore(filepath.Join(t.TempDir(), "store"), manifest)
	if err != nil {
		t.Fatalf("open piece store: %v", err)
	}
	state := &downloadPieceState{inProgress: map[int]bool{}}
	peers := []scheduler.PeerCandidate{
		{
			PeerID:     "udp://takeover-owner",
			Transport:  "udp",
			HaveRanges: []core.HaveRange{{Start: 1, End: 1}},
		},
		{
			PeerID:     "udp://other",
			Transport:  "udp",
			HaveRanges: []core.HaveRange{{Start: 0, End: 1}},
		},
	}

	pieceIndex, ok := reserveNextPiece(manifest, store, state, peers)
	if !ok {
		t.Fatal("expected piece reservation")
	}
	if pieceIndex != 1 {
		t.Fatalf("expected fallback rarest-first among owner-capable pieces to choose 1, got %d", pieceIndex)
	}
	delete(state.inProgress, pieceIndex)
	if err := store.PutPiece(1, []byte("done")); err != nil {
		t.Fatalf("put piece: %v", err)
	}

	pieceIndex, ok = reserveNextPiece(manifest, store, state, peers)
	if !ok {
		t.Fatal("expected second piece reservation")
	}
	if pieceIndex != 0 {
		t.Fatalf("expected fallback to remaining non-owner piece 0, got %d", pieceIndex)
	}
}

func TestReserveNextPieceSkipsTakeoverContinuityWhenQuotaFull(t *testing.T) {
	now := time.Now()
	contentID := "sha256-reserve-takeover-quota"
	ownerPeerID := "udp://takeover-owner"
	setUDPContentRouteTakeoverOwner(contentID, ownerPeerID, now)
	if !beginUDPContentRouteInflight(contentID, ownerPeerID, now) || !beginUDPContentRouteInflight(contentID, ownerPeerID, now) {
		t.Fatal("expected to fill takeover inflight quota")
	}

	manifest := &core.ContentManifest{
		Version:   1,
		ContentID: contentID,
		TotalSize: 8,
		PieceSize: 4,
		Pieces: []core.PieceInfo{
			{Index: 0, Length: 4, Hash: core.HashBytes([]byte("p000"))},
			{Index: 1, Length: 4, Hash: core.HashBytes([]byte("p111"))},
		},
	}
	store, err := core.OpenPieceStore(filepath.Join(t.TempDir(), "store"), manifest)
	if err != nil {
		t.Fatalf("open piece store: %v", err)
	}
	state := &downloadPieceState{inProgress: map[int]bool{}}
	peers := []scheduler.PeerCandidate{
		{PeerID: ownerPeerID, Transport: "udp", HaveRanges: []core.HaveRange{{Start: 1, End: 1}}},
		{PeerID: "udp://other", Transport: "udp", HaveRanges: []core.HaveRange{{Start: 0, End: 0}}},
	}

	pieceIndex, ok := reserveNextPiece(manifest, store, state, peers)
	if !ok {
		t.Fatal("expected reservation")
	}
	if pieceIndex != 0 {
		t.Fatalf("expected quota-full takeover to fall back to non-owner piece 0, got %d", pieceIndex)
	}
}

func TestPreferUDPContentTakeoverSelectionSkipsOwnerWhenQuotaFull(t *testing.T) {
	now := time.Now()
	contentID := "sha256-download-takeover-quota"
	handoffPeer := "udp://handoff-source"
	takeoverPeer := "udp://takeover-owner"

	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.131:9003", contentID, 1, true, now.Add(-6*time.Second))
	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.131:9003", contentID, 1, true, now.Add(-3*time.Second))
	noteUDPSessionPieceOwnership(handoffPeer, "198.51.100.131:9003", contentID, 1, false, now)
	setUDPContentRouteTakeoverOwner(contentID, takeoverPeer, now)
	if !beginUDPContentRouteInflight(contentID, takeoverPeer, now) || !beginUDPContentRouteInflight(contentID, takeoverPeer, now) {
		t.Fatal("expected to fill takeover inflight quota")
	}

	selected := scheduler.PeerCandidate{
		PeerID:     "tcp://steady",
		Transport:  "tcp",
		Score:      1.00,
		HaveRanges: []core.HaveRange{{Start: 0, End: 5}},
	}
	peers := []scheduler.PeerCandidate{
		selected,
		{
			PeerID:           takeoverPeer,
			Transport:        "udp",
			Score:            0.92,
			PeerTopologyRole: peerTopologyRoleBulk,
			PathAssistScore:  1.45,
			HaveRanges:       []core.HaveRange{{Start: 0, End: 5}},
		},
	}

	got := preferUDPContentTakeoverSelection(contentID, 2, selected, peers, now)
	if got.PeerID != selected.PeerID {
		t.Fatalf("expected quota-full takeover owner to stop overriding selection, got %+v", got)
	}
}

func TestUDPContentRouteInflightQuotaReleases(t *testing.T) {
	now := time.Now()
	contentID := "sha256-takeover-inflight-release"
	peerID := "udp://takeover-owner"
	setUDPContentRouteTakeoverOwner(contentID, peerID, now)

	if !beginUDPContentRouteInflight(contentID, peerID, now) || !beginUDPContentRouteInflight(contentID, peerID, now) {
		t.Fatal("expected inflight acquisitions up to quota")
	}
	if beginUDPContentRouteInflight(contentID, peerID, now) {
		t.Fatal("expected third inflight acquisition to fail at quota")
	}
	finishUDPContentRouteInflight(contentID, peerID, now.Add(time.Second))
	if !beginUDPContentRouteInflight(contentID, peerID, now.Add(2*time.Second)) {
		t.Fatal("expected inflight quota to reopen after release")
	}
}

func TestUDPContentRouteInflightQuotaExpandsForHealthySession(t *testing.T) {
	now := time.Now()
	contentID := "sha256-takeover-inflight-healthy"
	peerID := "udp://healthy-owner"

	noteUDPSessionStageSuccess(peerID, "198.51.100.141:9003", "probe", contentID, now.Add(-10*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.141:9003", "have", contentID, now.Add(-6*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.141:9003", "piece", contentID, now.Add(-2*time.Second))
	setUDPContentRouteTakeoverOwner(contentID, peerID, now)

	if !beginUDPContentRouteInflight(contentID, peerID, now) ||
		!beginUDPContentRouteInflight(contentID, peerID, now) ||
		!beginUDPContentRouteInflight(contentID, peerID, now) {
		t.Fatal("expected healthy takeover session to allow 3 inflight acquisitions")
	}
	if beginUDPContentRouteInflight(contentID, peerID, now) {
		t.Fatal("expected fourth inflight acquisition to fail")
	}
}

func TestUDPContentRouteInflightQuotaShrinksForHandoffSession(t *testing.T) {
	now := time.Now()
	contentID := "sha256-takeover-inflight-handoff"
	peerID := "udp://handoff-owner"

	noteUDPSessionPieceOwnership(peerID, "198.51.100.151:9003", contentID, 1, true, now.Add(-5*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.151:9003", contentID, 1, true, now.Add(-2*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.151:9003", contentID, 1, false, now)
	setUDPContentRouteTakeoverOwner(contentID, peerID, now)

	if !beginUDPContentRouteInflight(contentID, peerID, now) {
		t.Fatal("expected first inflight acquisition for handoff session")
	}
	if beginUDPContentRouteInflight(contentID, peerID, now) {
		t.Fatal("expected handoff session to cap inflight quota at 1")
	}
}

func TestUDPPieceChunkWindowAdjustsByStageRiskAndPublicMapping(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-window-stage"
	recordUDPBurstOutcome(contentID, "udp://piece-window-peer", "warm", "piece", true, now.Add(-2*time.Second))
	recordUDPBurstOutcome(contentID, "udp://probe-window-peer", "aggressive", "probe", false, now.Add(-2*time.Second))

	if got := udpPieceChunkWindowForCandidate(contentID, scheduler.PeerCandidate{
		Transport:       "udp",
		PeerID:          "udp://piece-window-peer",
		BurstProfile:    "warm",
		UDPDecisionRisk: "stable",
		UDPPublicMapped: true,
	}); got != 8 {
		t.Fatalf("expected warm/stable/public-mapped piece window 8, got %d", got)
	}

	if got := udpPieceChunkWindowForCandidate(contentID, scheduler.PeerCandidate{
		Transport:       "udp",
		PeerID:          "udp://probe-window-peer",
		BurstProfile:    "aggressive",
		UDPDecisionRisk: "low",
	}); got != 1 {
		t.Fatalf("expected aggressive/probe/low window 1, got %d", got)
	}
}

func TestUDPPieceChunkRoundTimeoutVariesByProfile(t *testing.T) {
	if got := udpPieceChunkRoundTimeoutForCandidate("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "warm"}); got != 700*time.Millisecond {
		t.Fatalf("expected warm chunk round timeout 700ms, got %s", got)
	}
	if got := udpPieceChunkRoundTimeoutForCandidate("", scheduler.PeerCandidate{Transport: "udp", BurstProfile: "aggressive"}); got != 1400*time.Millisecond {
		t.Fatalf("expected aggressive chunk round timeout 1.4s, got %s", got)
	}
	if got := udpPieceChunkRoundTimeoutForCandidate("", scheduler.PeerCandidate{Transport: "udp"}); got != time.Second {
		t.Fatalf("expected default chunk round timeout 1s, got %s", got)
	}
}

func TestUDPPieceChunkRoundTimeoutAdjustsByStageRiskAndPublicMapping(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-round-timeout-stage"
	recordUDPBurstOutcome(contentID, "udp://piece-round-peer", "warm", "piece", true, now.Add(-2*time.Second))
	recordUDPBurstOutcome(contentID, "udp://probe-round-peer", "aggressive", "probe", false, now.Add(-2*time.Second))

	if got := udpPieceChunkRoundTimeoutForCandidate(contentID, scheduler.PeerCandidate{
		Transport:       "udp",
		PeerID:          "udp://piece-round-peer",
		BurstProfile:    "warm",
		UDPDecisionRisk: "stable",
		UDPPublicMapped: true,
	}); got != 1050*time.Millisecond {
		t.Fatalf("expected warm/piece/stable/public-mapped round timeout 1050ms, got %s", got)
	}

	if got := udpPieceChunkRoundTimeoutForCandidate(contentID, scheduler.PeerCandidate{
		Transport:       "udp",
		PeerID:          "udp://probe-round-peer",
		BurstProfile:    "aggressive",
		UDPDecisionRisk: "low",
	}); got != 850*time.Millisecond {
		t.Fatalf("expected aggressive/probe/low round timeout 850ms, got %s", got)
	}
}

func TestUDPPieceChunkWindowAdjustsByRecentProgress(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-window-progress"

	recordUDPChunkProgress(contentID, "udp://fast-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        700 * time.Millisecond,
		Completed:       true,
	}, now)
	recordUDPChunkProgress(contentID, "udp://slow-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1300 * time.Millisecond,
	}, now)

	if got := udpPieceChunkWindowForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://fast-peer",
	}); got != 5 {
		t.Fatalf("expected recent fast progress to widen default chunk window to 5, got %d", got)
	}

	if got := udpPieceChunkWindowForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://slow-peer",
	}); got != 3 {
		t.Fatalf("expected weak progress to shrink default chunk window to 3, got %d", got)
	}
}

func TestUDPPieceChunkWindowAdjustsBySessionState(t *testing.T) {
	now := time.Now()
	activePeer := "udp://session-window-active"
	coolingPeer := "udp://session-window-cooling"

	noteUDPSessionSuccess(activePeer, "198.51.100.61:9003", "sha256-session-window", now.Add(-8*time.Second))
	noteUDPSessionSuccess(coolingPeer, "198.51.100.62:9003", "sha256-session-window", now.Add(-40*time.Second))
	noteUDPSessionFailure(coolingPeer, "198.51.100.62:9003", now.Add(-4*time.Second))

	if got := udpPieceChunkWindowForCandidate("", scheduler.PeerCandidate{Transport: "udp", PeerID: activePeer}); got != 5 {
		t.Fatalf("expected active session to widen default chunk window to 5, got %d", got)
	}
	if got := udpPieceChunkWindowForCandidate("", scheduler.PeerCandidate{Transport: "udp", PeerID: coolingPeer}); got != 3 {
		t.Fatalf("expected cooling session to shrink default chunk window to 3, got %d", got)
	}
}

func TestUDPPieceChunkWindowUsesStrongSessionRecommendation(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-window-strong"

	noteUDPSessionStageSuccess(peerID, "198.51.100.63:9003", "probe", "sha256-session-window", now.Add(-12*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.63:9003", "have", "sha256-session-window", now.Add(-8*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.63:9003", "piece", "sha256-session-window", now.Add(-4*time.Second))

	if got := udpPieceChunkWindowForCandidate("", scheduler.PeerCandidate{Transport: "udp", PeerID: peerID}); got != 6 {
		t.Fatalf("expected strong active session to widen default chunk window to 6, got %d", got)
	}
}

func TestUDPPieceChunkRoundTimeoutAdjustsByRecentProgress(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-round-progress"

	recordUDPChunkProgress(contentID, "udp://fast-round-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        600 * time.Millisecond,
		Completed:       true,
	}, now)
	recordUDPChunkProgress(contentID, "udp://empty-round-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 3,
		ReceivedChunks:  0,
		Duration:        1100 * time.Millisecond,
	}, now)

	if got := udpPieceChunkRoundTimeoutForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://fast-round-peer",
	}); got != 850*time.Millisecond {
		t.Fatalf("expected fast recent progress to tighten default round timeout to 850ms, got %s", got)
	}

	if got := udpPieceChunkRoundTimeoutForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://empty-round-peer",
	}); got != 1250*time.Millisecond {
		t.Fatalf("expected empty recent progress to widen default round timeout to 1250ms, got %s", got)
	}
}

func TestUDPPieceChunkRoundTimeoutAdjustsBySessionState(t *testing.T) {
	now := time.Now()
	activePeer := "udp://session-round-active"
	coolingPeer := "udp://session-round-cooling"

	noteUDPSessionSuccess(activePeer, "198.51.100.71:9003", "sha256-session-round", now.Add(-8*time.Second))
	noteUDPSessionSuccess(coolingPeer, "198.51.100.72:9003", "sha256-session-round", now.Add(-35*time.Second))
	noteUDPSessionFailure(coolingPeer, "198.51.100.72:9003", now.Add(-5*time.Second))

	if got := udpPieceChunkRoundTimeoutForCandidate("", scheduler.PeerCandidate{Transport: "udp", PeerID: activePeer}); got != 850*time.Millisecond {
		t.Fatalf("expected active session to tighten default round timeout to 850ms, got %s", got)
	}
	if got := udpPieceChunkRoundTimeoutForCandidate("", scheduler.PeerCandidate{Transport: "udp", PeerID: coolingPeer}); got != 1250*time.Millisecond {
		t.Fatalf("expected cooling session to widen default round timeout to 1250ms, got %s", got)
	}
}

func TestUDPPieceChunkRoundTimeoutUsesStrongSessionRecommendation(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-round-strong"

	noteUDPSessionStageSuccess(peerID, "198.51.100.73:9003", "probe", "sha256-session-round", now.Add(-12*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.73:9003", "have", "sha256-session-round", now.Add(-8*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.73:9003", "piece", "sha256-session-round", now.Add(-4*time.Second))

	if got := udpPieceChunkRoundTimeoutForCandidate("", scheduler.PeerCandidate{Transport: "udp", PeerID: peerID}); got != 800*time.Millisecond {
		t.Fatalf("expected strong active session to tighten default round timeout to 800ms, got %s", got)
	}
}

func TestRecentUDPChunkProgressExpires(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-progress-expire"
	recordUDPChunkProgress(contentID, "udp://stale-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 2,
		ReceivedChunks:  1,
		Duration:        time.Second,
	}, now.Add(-25*time.Second))

	if _, ok := recentUDPChunkProgress(contentID, "udp://stale-peer", now); ok {
		t.Fatal("expected stale chunk progress sample to expire")
	}
}

func TestUDPPieceChunkWindowAdjustsBySmoothedProgress(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-window-ewma"

	recordUDPChunkProgress(contentID, "udp://ewma-fast-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        720 * time.Millisecond,
		Completed:       true,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://ewma-fast-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 5,
		ReceivedChunks:  5,
		Duration:        760 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))

	recordUDPChunkProgress(contentID, "udp://ewma-weak-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1500 * time.Millisecond,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://ewma-weak-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1450 * time.Millisecond,
	}, now.Add(-1*time.Second))

	if got := udpPieceChunkWindowForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://ewma-fast-peer",
	}); got != 5 {
		t.Fatalf("expected smoothed strong progress to widen default window to 5, got %d", got)
	}

	if got := udpPieceChunkWindowForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://ewma-weak-peer",
	}); got != 3 {
		t.Fatalf("expected smoothed weak progress to shrink default window to 3, got %d", got)
	}
}

func TestUDPPieceChunkRoundTimeoutAdjustsBySmoothedProgress(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-round-ewma"

	recordUDPChunkProgress(contentID, "udp://ewma-fast-round-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        680 * time.Millisecond,
		Completed:       true,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://ewma-fast-round-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        740 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))

	recordUDPChunkProgress(contentID, "udp://ewma-slow-round-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1400 * time.Millisecond,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://ewma-slow-round-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1500 * time.Millisecond,
	}, now.Add(-1*time.Second))

	if got := udpPieceChunkRoundTimeoutForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://ewma-fast-round-peer",
	}); got != 850*time.Millisecond {
		t.Fatalf("expected smoothed strong progress to tighten default round timeout to 850ms, got %s", got)
	}

	if got := udpPieceChunkRoundTimeoutForCandidate(contentID, scheduler.PeerCandidate{
		Transport: "udp",
		PeerID:    "udp://ewma-slow-round-peer",
	}); got != 1250*time.Millisecond {
		t.Fatalf("expected smoothed weak progress to widen default round timeout to 1250ms, got %s", got)
	}
}

func TestSmoothedUDPChunkProgressExpires(t *testing.T) {
	now := time.Now()
	const contentID = "sha256-download-ewma-expire"
	recordUDPChunkProgress(contentID, "udp://stale-ewma-peer", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        700 * time.Millisecond,
		Completed:       true,
	}, now.Add(-50*time.Second))

	if _, ok := smoothedUDPChunkProgress(contentID, "udp://stale-ewma-peer", now); ok {
		t.Fatal("expected stale smoothed chunk progress to expire")
	}
}
