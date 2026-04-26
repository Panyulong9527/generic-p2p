package main

import (
	"testing"
	"time"

	"generic-p2p/internal/tracker"
)

func TestUDPCandidateScorePrefersFreshObservedAddresses(t *testing.T) {
	now := time.Unix(100, 0)
	observed := map[string]udpPeerPreference{
		"198.51.100.10:9003": {observedAt: now.Add(-3 * time.Second)},
		"198.51.100.11:9003": {observedAt: now.Add(-10 * time.Second)},
		"198.51.100.12:9003": {observedAt: now.Add(-20 * time.Second)},
	}

	if got := udpCandidateScore("198.51.100.10:9003", observed, now); got != 1.8 {
		t.Fatalf("expected freshest observed udp score 1.8, got %.1f", got)
	}
	if got := udpCandidateScore("198.51.100.11:9003", observed, now); got != 1.6 {
		t.Fatalf("expected medium-age observed udp score 1.6, got %.1f", got)
	}
	if got := udpCandidateScore("198.51.100.12:9003", observed, now); got != 1.4 {
		t.Fatalf("expected older observed udp score 1.4, got %.1f", got)
	}
	if got := udpCandidateScore("203.0.113.20:9003", observed, now); got != 1.2 {
		t.Fatalf("expected default udp score 1.2, got %.1f", got)
	}
}

func TestTrackerUDPProbeBiasPrefersRecentSuccessAndPenalizesRecentFailure(t *testing.T) {
	now := time.Unix(200, 0)

	success := trackerUDPProbeBias(tracker.UDPProbeResultStatus{
		TargetPeerID:  "peer-a",
		LastSuccessAt: now.Add(-5 * time.Second).Unix(),
	}, now)
	if success != 0.25 {
		t.Fatalf("expected recent success bias 0.25, got %.2f", success)
	}

	failure := trackerUDPProbeBias(tracker.UDPProbeResultStatus{
		TargetPeerID:  "peer-b",
		LastFailureAt: now.Add(-8 * time.Second).Unix(),
		LastErrorKind: "udp_timeout",
		FailureCount:  2,
		SuccessCount:  0,
	}, now)
	if failure != -0.25 {
		t.Fatalf("expected recent failure bias -0.25, got %.2f", failure)
	}
}
