package main

import "testing"
import "time"

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
