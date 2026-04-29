package main

import (
	"testing"
	"time"

	p2pnet "generic-p2p/internal/net"
)

func TestUDPSessionPrefersLatestSuccessfulAddr(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-peer-latest"

	noteUDPSessionAddr(peerID, "198.51.100.10:9003", "declared_udp", "sha256-demo", now.Add(-5*time.Second))
	noteUDPSessionSuccess(peerID, "198.51.100.11:9003", "sha256-demo", now)

	addr, ok := udpSessionPreferredAddr(peerID, now)
	if !ok {
		t.Fatal("expected preferred session addr")
	}
	if addr != "198.51.100.11:9003" {
		t.Fatalf("expected latest successful addr, got %s", addr)
	}
}

func TestUDPSessionWarmKeepalivePeersReturnsRecentSuccessfulSession(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-peer-warm"

	noteUDPSessionSuccess(peerID, "198.51.100.20:9003", "sha256-demo", now.Add(-10*time.Second))

	sessions := udpWarmSessionPeers("sha256-demo", now)
	found := false
	for _, session := range sessions {
		if session.PeerID == peerID && session.PrimaryAddr == "198.51.100.20:9003" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected warm session for %s, got %+v", peerID, sessions)
	}
}

func TestUDPSessionDiscoveryBiasRewardsRecentWarmSession(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-peer-bias"
	contentID := "sha256-session-bias"

	noteUDPSessionSuccess(peerID, "198.51.100.30:9003", contentID, now.Add(-8*time.Second))

	if got := udpSessionDiscoveryBias(contentID, peerID, now); got != 0.10 {
		t.Fatalf("expected recent active session bias 0.10, got %.2f", got)
	}
}

func TestUDPSessionExpiresAfterLongIdle(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-peer-expire"

	noteUDPSessionSuccess(peerID, "198.51.100.40:9003", "sha256-expire", now.Add(-3*time.Minute))

	if _, ok := udpSessionPreferredAddr(peerID, now); ok {
		t.Fatal("expected long-idle session to expire")
	}
}

func TestUDPSessionStateTransitions(t *testing.T) {
	now := time.Now()

	activePeer := "udp://session-state-active"
	noteUDPSessionSuccess(activePeer, "198.51.100.51:9003", "sha256-demo", now.Add(-8*time.Second))
	if got := udpSessionStateForPeer(activePeer, now); got != "active" {
		t.Fatalf("expected active session state, got %s", got)
	}

	warmPeer := "udp://session-state-warm"
	noteUDPSessionSuccess(warmPeer, "198.51.100.52:9003", "sha256-demo", now.Add(-45*time.Second))
	if got := udpSessionStateForPeer(warmPeer, now); got != "warm" {
		t.Fatalf("expected warm session state, got %s", got)
	}

	coolingPeer := "udp://session-state-cooling"
	noteUDPSessionSuccess(coolingPeer, "198.51.100.53:9003", "sha256-demo", now.Add(-40*time.Second))
	noteUDPSessionFailure(coolingPeer, "198.51.100.53:9003", now.Add(-5*time.Second))
	if got := udpSessionStateForPeer(coolingPeer, now); got != "cooling" {
		t.Fatalf("expected cooling session state, got %s", got)
	}

	activeSession, ok := udpSessionSnapshot(activePeer, now)
	if !ok {
		t.Fatal("expected active session snapshot")
	}
	if activeSession.RecommendedChunkWindow != 5 || activeSession.RecommendedAttemptBudget != 4 || activeSession.RecommendedRoundTimeout != 850*time.Millisecond {
		t.Fatalf("unexpected active session recommendations: %+v", activeSession)
	}

	warmSession, ok := udpSessionSnapshot(warmPeer, now)
	if !ok {
		t.Fatal("expected warm session snapshot")
	}
	if warmSession.RecommendedChunkWindow != 4 || warmSession.RecommendedAttemptBudget != 3 || warmSession.RecommendedRoundTimeout != 950*time.Millisecond {
		t.Fatalf("unexpected warm session recommendations: %+v", warmSession)
	}

	coolingSession, ok := udpSessionSnapshot(coolingPeer, now)
	if !ok {
		t.Fatal("expected cooling session snapshot")
	}
	if coolingSession.RecommendedChunkWindow != 3 || coolingSession.RecommendedAttemptBudget != 2 || coolingSession.RecommendedRoundTimeout != 1250*time.Millisecond {
		t.Fatalf("unexpected cooling session recommendations: %+v", coolingSession)
	}
}

func TestUDPSessionHealthTracksStageEvents(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-health-stage"

	noteUDPSessionStageSuccess(peerID, "198.51.100.61:9003", "probe", "sha256-demo", now.Add(-12*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.61:9003", "have", "sha256-demo", now.Add(-8*time.Second))
	noteUDPSessionStageSuccess(peerID, "198.51.100.61:9003", "piece", "sha256-demo", now.Add(-4*time.Second))

	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.HealthScore <= 0.4 {
		t.Fatalf("expected positive health score after staged successes, got %.2f", session.HealthScore)
	}
	if session.LastStage != "piece" {
		t.Fatalf("expected last stage piece, got %s", session.LastStage)
	}
	if session.State != "active" {
		t.Fatalf("expected active state after staged successes, got %s", session.State)
	}
	if session.RecommendedChunkWindow != 6 || session.RecommendedAttemptBudget != 5 || session.RecommendedRoundTimeout != 800*time.Millisecond {
		t.Fatalf("expected stronger active recommendations after healthy stages, got %+v", session)
	}
}

func TestUDPSessionHealthCoolsAfterRepeatedFailures(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-health-cooling"

	noteUDPSessionStageSuccess(peerID, "198.51.100.62:9003", "piece", "sha256-demo", now.Add(-20*time.Second))
	noteUDPSessionStageFailure(peerID, "198.51.100.62:9003", "keepalive", "udp_timeout", now.Add(-6*time.Second))
	noteUDPSessionStageFailure(peerID, "198.51.100.62:9003", "probe", "udp_timeout", now.Add(-2*time.Second))

	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.HealthScore >= 0 {
		t.Fatalf("expected negative health after repeated failures, got %.2f", session.HealthScore)
	}
	if session.State != "cooling" {
		t.Fatalf("expected cooling state after repeated failures, got %s", session.State)
	}
	if session.LastErrorKind != "udp_timeout" {
		t.Fatalf("expected last error kind udp_timeout, got %s", session.LastErrorKind)
	}
}

func TestUDPSessionPieceRoundImprovesRecommendations(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-piece-round-good"

	noteUDPSessionSuccess(peerID, "198.51.100.71:9003", "sha256-demo", now.Add(-5*time.Second))
	noteUDPSessionPieceRound(peerID, "198.51.100.71:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        720 * time.Millisecond,
		Completed:       true,
	}, now)

	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.RecommendedChunkWindow < 6 {
		t.Fatalf("expected chunk round success to widen session window, got %+v", session)
	}
	if session.RecommendedRoundTimeout > 750*time.Millisecond {
		t.Fatalf("expected chunk round success to tighten session timeout, got %+v", session)
	}
	if session.RecommendedAttemptBudget < 5 {
		t.Fatalf("expected chunk round success to widen session budget, got %+v", session)
	}
}

func TestUDPSessionPieceRoundCoolsRecommendations(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-piece-round-bad"

	noteUDPSessionSuccess(peerID, "198.51.100.72:9003", "sha256-demo", now.Add(-5*time.Second))
	noteUDPSessionPieceRound(peerID, "198.51.100.72:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  0,
		Duration:        1300 * time.Millisecond,
	}, now)

	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.RecommendedChunkWindow > 4 {
		t.Fatalf("expected failed chunk round to cool session window, got %+v", session)
	}
	if session.RecommendedRoundTimeout < time.Second {
		t.Fatalf("expected failed chunk round to widen session timeout, got %+v", session)
	}
	if session.RecommendedAttemptBudget > 3 {
		t.Fatalf("expected failed chunk round to cool session budget, got %+v", session)
	}
}

func TestUDPSessionPieceSuccessCreatesStickyBulkAffinity(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-sticky-bulk"
	contentID := "sha256-sticky-bulk"

	noteUDPSessionStageSuccess(peerID, "198.51.100.81:9003", "piece", contentID, now.Add(-2*time.Second))

	if got := udpSessionStickyRole(peerID, contentID, now); got != "bulk" {
		t.Fatalf("expected sticky bulk affinity, got %s", got)
	}
	if udpSessionIsQuarantined(peerID, now) {
		t.Fatal("did not expect successful piece session to be quarantined")
	}
}

func TestUDPSessionRepeatedPieceFailuresCreateFallbackQuarantine(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-sticky-fallback"
	contentID := "sha256-sticky-fallback"

	noteUDPSessionStageSuccess(peerID, "198.51.100.82:9003", "piece", contentID, now.Add(-20*time.Second))
	noteUDPSessionStageFailure(peerID, "198.51.100.82:9003", "piece", "udp_timeout", now.Add(-4*time.Second))
	noteUDPSessionStageFailure(peerID, "198.51.100.82:9003", "piece", "udp_timeout", now.Add(-1*time.Second))

	if got := udpSessionStickyRole(peerID, contentID, now); got != "fallback" {
		t.Fatalf("expected sticky fallback affinity after failures, got %s", got)
	}
	if !udpSessionIsQuarantined(peerID, now) {
		t.Fatal("expected repeated piece failures to quarantine session")
	}
}

func TestUDPSessionPipelineBoostCarriesAcrossPieces(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-pipeline-boost"
	contentID := "sha256-pipeline-boost"

	noteUDPSessionSuccess(peerID, "198.51.100.83:9003", contentID, now.Add(-5*time.Second))
	noteUDPSessionPieceRound(peerID, "198.51.100.83:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        700 * time.Millisecond,
		Completed:       true,
	}, now)

	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if !udpSessionPipelineActive(session, now) {
		t.Fatal("expected pipeline boost to become active after strong round")
	}
	if session.PipelineDepth < 1 {
		t.Fatalf("expected positive pipeline depth, got %+v", session)
	}
	if udpSessionWindowBias(peerID, now) < 3 {
		t.Fatalf("expected boosted session window bias after pipeline warmup, got %d", udpSessionWindowBias(peerID, now))
	}
	if udpSessionAttemptBudgetBias(peerID, now) < 3 {
		t.Fatalf("expected boosted session budget bias after pipeline warmup, got %d", udpSessionAttemptBudgetBias(peerID, now))
	}
}

func TestUDPSessionPipelineBoostCoolsAfterEmptyRound(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-pipeline-cool"
	contentID := "sha256-pipeline-cool"

	noteUDPSessionSuccess(peerID, "198.51.100.84:9003", contentID, now.Add(-6*time.Second))
	noteUDPSessionPieceRound(peerID, "198.51.100.84:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        720 * time.Millisecond,
		Completed:       true,
	}, now.Add(-2*time.Second))
	noteUDPSessionPieceRound(peerID, "198.51.100.84:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  0,
		Duration:        1400 * time.Millisecond,
	}, now)

	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.PipelineDepth != 0 {
		t.Fatalf("expected empty round to clear pipeline depth, got %+v", session)
	}
	if udpSessionPipelineActive(session, now) {
		t.Fatal("expected pipeline boost to cool down after empty round")
	}
}

func TestUDPSessionPieceOwnershipBuildsConsecutiveRuns(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-owner-run"
	contentID := "sha256-owner-run"

	noteUDPSessionPieceOwnership(peerID, "198.51.100.85:9003", contentID, 2, true, now.Add(-3*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.85:9003", contentID, 2, true, now)

	if got := udpSessionOwnerRun(peerID, contentID, 2, now); got != 2 {
		t.Fatalf("expected owner run length 2, got %d", got)
	}
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.ConsecutivePieceRuns != 2 {
		t.Fatalf("expected consecutive piece runs 2, got %+v", session)
	}
	if session.RecommendedAttemptBudget < 5 {
		t.Fatalf("expected owner run to strengthen attempt budget, got %+v", session)
	}
	if got := udpSessionContentRun(peerID, contentID, now); got != 2 {
		t.Fatalf("expected content run length 2, got %d", got)
	}
}

func TestUDPSessionPieceOwnershipFailureClearsRun(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-owner-reset"
	contentID := "sha256-owner-reset"

	noteUDPSessionPieceOwnership(peerID, "198.51.100.86:9003", contentID, 1, true, now.Add(-3*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.86:9003", contentID, 1, false, now)

	if got := udpSessionOwnerRun(peerID, contentID, 1, now); got != 0 {
		t.Fatalf("expected owner run to clear after failure, got %d", got)
	}
}

func TestUDPSessionContentRunSharedAcrossWorkers(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-content-run"
	contentID := "sha256-content-run"

	noteUDPSessionPieceOwnership(peerID, "198.51.100.87:9003", contentID, 1, true, now.Add(-4*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.87:9003", contentID, 2, true, now)

	if got := udpSessionContentRun(peerID, contentID, now); got != 2 {
		t.Fatalf("expected shared content run length 2, got %d", got)
	}
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if session.ContentPieceRuns != 2 {
		t.Fatalf("expected content piece runs 2, got %+v", session)
	}
	if session.RecommendedChunkWindow < 6 {
		t.Fatalf("expected content run to widen session window, got %+v", session)
	}
}

func TestUDPSessionContentRunFailureEntersHandoff(t *testing.T) {
	now := time.Now()
	peerID := "udp://session-handoff"
	contentID := "sha256-session-handoff"

	noteUDPSessionPieceOwnership(peerID, "198.51.100.88:9003", contentID, 1, true, now.Add(-5*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.88:9003", contentID, 2, true, now.Add(-2*time.Second))
	noteUDPSessionPieceOwnership(peerID, "198.51.100.88:9003", contentID, 2, false, now)

	if !udpSessionInHandoff(peerID, contentID, now) {
		t.Fatal("expected session to enter handoff after content-run failure")
	}
	if got := udpSessionContentRun(peerID, contentID, now); got != 1 {
		t.Fatalf("expected content run to decay to 1, got %d", got)
	}
}
