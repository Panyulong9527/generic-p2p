package main

import (
	"math"
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
	if failure != -0.18 {
		t.Fatalf("expected udp timeout failure bias -0.18, got %.2f", failure)
	}
}

func TestTrackerUDPProbeBiasPenalizesGenericFailuresMoreHeavily(t *testing.T) {
	now := time.Unix(300, 0)

	failure := trackerUDPProbeBias(tracker.UDPProbeResultStatus{
		TargetPeerID:  "peer-c",
		LastFailureAt: now.Add(-6 * time.Second).Unix(),
		LastErrorKind: "generic",
	}, now)
	if failure != -0.30 {
		t.Fatalf("expected generic failure bias -0.30, got %.2f", failure)
	}

	older := trackerUDPProbeBias(tracker.UDPProbeResultStatus{
		TargetPeerID:  "peer-d",
		LastFailureAt: now.Add(-20 * time.Second).Unix(),
		LastErrorKind: "generic",
	}, now)
	if older != -0.18 {
		t.Fatalf("expected decayed generic failure bias -0.18, got %.2f", older)
	}
}

func TestBuildTrackerUDPPeerBiasesIncludesRecentTransferDrift(t *testing.T) {
	now := time.Unix(400, 0)
	status := tracker.StatusResponse{
		UDPProbeResults: []tracker.UDPProbeResultStatus{
			{
				TargetPeerID:  "peer-a",
				LastSuccessAt: now.Add(-5 * time.Second).Unix(),
			},
			{
				TargetPeerID:  "peer-b",
				LastSuccessAt: now.Add(-5 * time.Second).Unix(),
			},
			{
				TargetPeerID:  "peer-c",
				LastFailureAt: now.Add(-4 * time.Second).Unix(),
				LastErrorKind: "generic",
			},
		},
		PeerTransferPaths: []tracker.PeerTransferPathStatus{
			{
				TargetPeerID: "peer-a",
				ContentID:    "sha256-demo",
				LastPath:     "tcp",
				LastAt:       now.Add(-8 * time.Second).Unix(),
				TCPCount:     3,
			},
			{
				TargetPeerID: "peer-b",
				ContentID:    "sha256-demo",
				LastPath:     "udp",
				LastAt:       now.Add(-6 * time.Second).Unix(),
				UDPCount:     2,
			},
			{
				TargetPeerID: "peer-c",
				ContentID:    "sha256-demo",
				LastPath:     "udp",
				LastAt:       now.Add(-7 * time.Second).Unix(),
				UDPCount:     1,
			},
		},
		Swarms: []tracker.SwarmStatus{
			{
				ContentID: "sha256-demo",
				Peers: []tracker.PeerRecord{
					{PeerID: "peer-a", UDPAddrs: []string{"198.51.100.1:9003"}},
					{PeerID: "peer-b", UDPAddrs: []string{"198.51.100.2:9003"}},
					{PeerID: "peer-c", UDPAddrs: []string{"198.51.100.3:9003"}},
				},
			},
		},
	}

	biases := buildTrackerUDPPeerBiases(status, "sha256-demo", now)
	if got := biases["peer-a"]; math.Abs(got-(-0.03)) > 1e-9 {
		t.Fatalf("expected peer-a combined bias -0.03, got %.2f", got)
	}
	if got := biases["peer-b"]; math.Abs(got-0.25) > 1e-9 {
		t.Fatalf("expected peer-b combined bias 0.25, got %.2f", got)
	}
	if got := biases["peer-c"]; math.Abs(got-(-0.18)) > 1e-9 {
		t.Fatalf("expected peer-c combined bias -0.18, got %.2f", got)
	}
}

func TestTrackerTransferPathBiasDecaysAndIgnoresOtherContent(t *testing.T) {
	now := time.Unix(500, 0)
	status := tracker.StatusResponse{
		PeerTransferPaths: []tracker.PeerTransferPathStatus{
			{
				TargetPeerID: "peer-a",
				ContentID:    "sha256-other",
				LastPath:     "tcp",
				LastAt:       now.Add(-5 * time.Second).Unix(),
			},
			{
				TargetPeerID: "peer-b",
				ContentID:    "sha256-demo",
				LastPath:     "tcp",
				LastAt:       now.Add(-45 * time.Second).Unix(),
			},
		},
		Swarms: []tracker.SwarmStatus{
			{
				ContentID: "sha256-demo",
				Peers: []tracker.PeerRecord{
					{PeerID: "peer-a", UDPAddrs: []string{"198.51.100.1:9003"}},
					{PeerID: "peer-b", UDPAddrs: []string{"198.51.100.2:9003"}},
				},
			},
		},
	}

	biases := buildTrackerUDPPeerBiases(status, "sha256-demo", now)
	if got := biases["peer-a"]; math.Abs(got) > 1e-9 {
		t.Fatalf("expected peer-a other-content transfer bias ignored, got %.2f", got)
	}
	if got := biases["peer-b"]; math.Abs(got-(-0.10)) > 1e-9 {
		t.Fatalf("expected peer-b older udp miss bias -0.10, got %.2f", got)
	}
}

func TestBuildTrackerUDPPeerBiasesIncludesKeepaliveBias(t *testing.T) {
	now := time.Unix(600, 0)
	status := tracker.StatusResponse{
		UDPKeepaliveResults: []tracker.UDPKeepaliveStatus{
			{
				TargetPeerID:  "udp://198.51.100.1:9003",
				ContentID:     "sha256-demo",
				SuccessCount:  2,
				LastSuccessAt: now.Add(-5 * time.Second).Unix(),
			},
			{
				TargetPeerID:  "udp://198.51.100.2:9003",
				ContentID:     "sha256-demo",
				FailureCount:  1,
				LastFailureAt: now.Add(-8 * time.Second).Unix(),
				LastErrorKind: "udp_timeout",
			},
			{
				TargetPeerID:  "udp://198.51.100.3:9003",
				ContentID:     "sha256-other",
				FailureCount:  1,
				LastFailureAt: now.Add(-6 * time.Second).Unix(),
				LastErrorKind: "generic",
			},
		},
		Swarms: []tracker.SwarmStatus{
			{
				ContentID: "sha256-demo",
				Peers: []tracker.PeerRecord{
					{PeerID: "peer-a", UDPAddrs: []string{"198.51.100.1:9003"}},
					{PeerID: "peer-b", UDPAddrs: []string{"198.51.100.2:9003"}},
				},
			},
		},
	}

	biases := buildTrackerUDPPeerBiases(status, "sha256-demo", now)
	if got := biases["peer-a"]; math.Abs(got-0.10) > 1e-9 {
		t.Fatalf("expected keepalive success bias 0.10 on peer-a, got %.2f", got)
	}
	if got := biases["peer-b"]; math.Abs(got-(-0.12)) > 1e-9 {
		t.Fatalf("expected keepalive timeout bias -0.12 on peer-b, got %.2f", got)
	}
	if got := biases["peer-c"]; math.Abs(got) > 1e-9 {
		t.Fatalf("expected missing peer keepalive bias ignored, got %.2f", got)
	}
}

func TestBuildTrackerUDPPeerBiasesAddsFallbackPenaltyForTcpMissAndKeepaliveFailure(t *testing.T) {
	now := time.Unix(700, 0)
	status := tracker.StatusResponse{
		UDPProbeResults: []tracker.UDPProbeResultStatus{
			{
				TargetPeerID:  "peer-a",
				LastSuccessAt: now.Add(-6 * time.Second).Unix(),
			},
		},
		PeerTransferPaths: []tracker.PeerTransferPathStatus{
			{
				TargetPeerID: "peer-a",
				ContentID:    "sha256-demo",
				LastPath:     "tcp",
				LastAt:       now.Add(-5 * time.Second).Unix(),
				TCPCount:     4,
			},
		},
		UDPKeepaliveResults: []tracker.UDPKeepaliveStatus{
			{
				TargetPeerID:  "udp://198.51.100.1:9003",
				ContentID:     "sha256-demo",
				FailureCount:  2,
				LastFailureAt: now.Add(-4 * time.Second).Unix(),
				LastErrorKind: "udp_timeout",
			},
		},
		Swarms: []tracker.SwarmStatus{
			{
				ContentID: "sha256-demo",
				Peers: []tracker.PeerRecord{
					{PeerID: "peer-a", UDPAddrs: []string{"198.51.100.1:9003"}},
				},
			},
		},
	}

	biases := buildTrackerUDPPeerBiases(status, "sha256-demo", now)
	if got := biases["peer-a"]; math.Abs(got-(-0.31)) > 1e-9 {
		t.Fatalf("expected combined fallback bias -0.31, got %.2f", got)
	}
}
