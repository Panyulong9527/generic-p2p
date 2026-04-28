package main

import (
	"math"
	"reflect"
	"testing"
	"time"

	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/tracker"
)

func TestUDPCandidateScorePrefersFreshObservedAddresses(t *testing.T) {
	now := time.Unix(100, 0)
	observed := map[string]udpPeerPreference{
		"198.51.100.10:9003": {observedAt: now.Add(-3 * time.Second), source: "local_observed"},
		"198.51.100.11:9003": {observedAt: now.Add(-10 * time.Second), source: "local_observed"},
		"198.51.100.12:9003": {observedAt: now.Add(-20 * time.Second), source: "local_observed"},
	}

	if got := udpCandidateScore("", "198.51.100.10:9003", observed, now); got != 1.8 {
		t.Fatalf("expected freshest observed udp score 1.8, got %.1f", got)
	}
	if got := udpCandidateScore("", "198.51.100.11:9003", observed, now); got != 1.6 {
		t.Fatalf("expected medium-age observed udp score 1.6, got %.1f", got)
	}
	if got := udpCandidateScore("", "198.51.100.12:9003", observed, now); got != 1.4 {
		t.Fatalf("expected older observed udp score 1.4, got %.1f", got)
	}
	if got := udpCandidateScore("", "203.0.113.20:9003", observed, now); got != 1.2 {
		t.Fatalf("expected default udp score 1.2, got %.1f", got)
	}
}

func TestUDPCandidateScoreSeparatesSTUNTrackerAndDeclaredSources(t *testing.T) {
	now := time.Unix(120, 0)
	preferences := map[string]udpPeerPreference{
		"198.51.100.21:9003": {source: "stun"},
		"198.51.100.22:9003": {source: "tracker"},
		"198.51.100.23:9003": {source: "declared_udp"},
	}

	if got := udpCandidateScore("", "198.51.100.21:9003", preferences, now); got != 1.5 {
		t.Fatalf("expected stun-observed score 1.5, got %.2f", got)
	}
	if got := udpCandidateScore("", "198.51.100.22:9003", preferences, now); got != 1.35 {
		t.Fatalf("expected tracker-observed score 1.35, got %.2f", got)
	}
	if got := udpCandidateScore("", "198.51.100.23:9003", preferences, now); got != 1.2 {
		t.Fatalf("expected declared udp score 1.2, got %.2f", got)
	}
}

func TestUDPCandidateScoreAdjustsBySmoothedChunkProgress(t *testing.T) {
	now := time.Unix(140, 0)
	const contentID = "sha256-discovery-progress"
	preferences := map[string]udpPeerPreference{
		"198.51.100.31:9003": {source: "declared_udp"},
		"198.51.100.32:9003": {source: "declared_udp"},
	}

	recordUDPChunkProgress(contentID, "udp://198.51.100.31:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        700 * time.Millisecond,
		Completed:       true,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://198.51.100.31:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        760 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))

	recordUDPChunkProgress(contentID, "udp://198.51.100.32:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1500 * time.Millisecond,
	}, now.Add(-3*time.Second))
	recordUDPChunkProgress(contentID, "udp://198.51.100.32:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  1,
		Duration:        1450 * time.Millisecond,
	}, now.Add(-1*time.Second))

	if got := udpCandidateScore(contentID, "198.51.100.31:9003", preferences, now); !nearlyEqualFloat64(got, 1.32) {
		t.Fatalf("expected healthy smoothed chunk progress to raise declared udp score to 1.32, got %.4f", got)
	}
	if got := udpCandidateScore(contentID, "198.51.100.32:9003", preferences, now); !nearlyEqualFloat64(got, 1.08) {
		t.Fatalf("expected weak smoothed chunk progress to lower declared udp score to 1.08, got %.4f", got)
	}
}

func TestUDPCandidateScoreKeepsSourcePriorityWithRecentChunkProgress(t *testing.T) {
	now := time.Unix(160, 0)
	const contentID = "sha256-discovery-source-priority"
	preferences := map[string]udpPeerPreference{
		"198.51.100.41:9003": {source: "stun"},
		"198.51.100.42:9003": {source: "declared_udp"},
	}

	recordUDPChunkProgress(contentID, "udp://198.51.100.42:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        680 * time.Millisecond,
		Completed:       true,
	}, now.Add(-2*time.Second))
	recordUDPChunkProgress(contentID, "udp://198.51.100.42:9003", p2pnet.UDPPieceRoundStats{
		RequestedChunks: 4,
		ReceivedChunks:  4,
		Duration:        720 * time.Millisecond,
		Completed:       true,
	}, now.Add(-1*time.Second))

	stunScore := udpCandidateScore(contentID, "198.51.100.41:9003", preferences, now)
	declaredScore := udpCandidateScore(contentID, "198.51.100.42:9003", preferences, now)
	if stunScore <= declaredScore {
		t.Fatalf("expected stun source %.2f to stay ahead of declared source %.2f even after chunk bias", stunScore, declaredScore)
	}
}

func nearlyEqualFloat64(left float64, right float64) bool {
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.000001
}

func TestUDPDiscoveryTimeoutsVaryByBurstProfile(t *testing.T) {
	if got := udpProbeTimeoutForProfile("warm"); got != 2200*time.Millisecond {
		t.Fatalf("expected warm probe timeout 2.2s, got %s", got)
	}
	if got := udpProbeTimeoutForProfile("aggressive"); got != 3800*time.Millisecond {
		t.Fatalf("expected aggressive probe timeout 3.8s, got %s", got)
	}
	if got := udpProbeTimeoutForProfile(""); got != 3*time.Second {
		t.Fatalf("expected default probe timeout 3s, got %s", got)
	}

	if got := udpHaveTimeoutForProfile("warm"); got != 2600*time.Millisecond {
		t.Fatalf("expected warm have timeout 2.6s, got %s", got)
	}
	if got := udpHaveTimeoutForProfile("aggressive"); got != 4300*time.Millisecond {
		t.Fatalf("expected aggressive have timeout 4.3s, got %s", got)
	}
	if got := udpHaveTimeoutForProfile(""); got != 3400*time.Millisecond {
		t.Fatalf("expected default have timeout 3.4s, got %s", got)
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

func TestBuildTrackerUDPPeerBiasesIncludesBurstProfileBias(t *testing.T) {
	now := time.Unix(750, 0)
	status := tracker.StatusResponse{
		UDPBurstProfiles: []tracker.UDPBurstProfileStatus{
			{
				TargetPeerID:   "peer-a",
				ContentID:      "sha256-demo",
				Profile:        "warm",
				LastOutcome:    "success",
				LastStage:      "have",
				LastReportedAt: now.Add(-5 * time.Second).Unix(),
			},
			{
				TargetPeerID:   "peer-b",
				ContentID:      "sha256-demo",
				Profile:        "aggressive",
				LastOutcome:    "failure",
				LastStage:      "probe",
				FailureCount:   2,
				LastReportedAt: now.Add(-6 * time.Second).Unix(),
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
	if got := biases["peer-a"]; math.Abs(got-0.11) > 1e-9 {
		t.Fatalf("expected warm have-success burst bias 0.11, got %.2f", got)
	}
	if got := biases["peer-b"]; math.Abs(got-(-0.18)) > 1e-9 {
		t.Fatalf("expected aggressive probe-failure burst bias -0.18, got %.2f", got)
	}
}

func TestTrackerBurstProfileStageBiasPenalizesHaveFailuresLessThanProbeFailures(t *testing.T) {
	now := time.Unix(760, 0)
	probeFailure := trackerBurstProfileBias(tracker.UDPBurstProfileStatus{
		Profile:        "aggressive",
		LastOutcome:    "failure",
		LastStage:      "probe",
		LastReportedAt: now.Add(-4 * time.Second).Unix(),
	}, now)
	haveFailure := trackerBurstProfileBias(tracker.UDPBurstProfileStatus{
		Profile:        "aggressive",
		LastOutcome:    "failure",
		LastStage:      "have",
		LastReportedAt: now.Add(-4 * time.Second).Unix(),
	}, now)
	pieceFailure := trackerBurstProfileBias(tracker.UDPBurstProfileStatus{
		Profile:        "aggressive",
		LastOutcome:    "failure",
		LastStage:      "piece",
		LastReportedAt: now.Add(-4 * time.Second).Unix(),
	}, now)
	pieceSuccess := trackerBurstProfileBias(tracker.UDPBurstProfileStatus{
		Profile:        "warm",
		LastOutcome:    "success",
		LastStage:      "piece",
		LastReportedAt: now.Add(-4 * time.Second).Unix(),
	}, now)

	if !(probeFailure < haveFailure && haveFailure < pieceFailure) {
		t.Fatalf("expected stage penalties probe < have < piece, got probe=%.2f have=%.2f piece=%.2f", probeFailure, haveFailure, pieceFailure)
	}
	if math.Abs(pieceSuccess-0.14) > 1e-9 {
		t.Fatalf("expected warm piece-success bias 0.14, got %.2f", pieceSuccess)
	}
}

func TestBuildTrackerUDPPeerBiasesIncludesUDPDecisionRiskBias(t *testing.T) {
	now := time.Unix(770, 0)
	status := tracker.StatusResponse{
		UDPProbeResults: []tracker.UDPProbeResultStatus{
			{TargetPeerID: "peer-low", LastFailureAt: now.Add(-5 * time.Second).Unix(), LastErrorKind: "udp_timeout"},
			{TargetPeerID: "peer-stable", LastSuccessAt: now.Add(-4 * time.Second).Unix()},
			{TargetPeerID: "peer-recover", LastFailureAt: now.Add(-6 * time.Second).Unix(), LastErrorKind: "udp_timeout"},
		},
		PeerTransferPaths: []tracker.PeerTransferPathStatus{
			{TargetPeerID: "peer-low", ContentID: "sha256-demo", LastPath: "tcp", LastAt: now.Add(-3 * time.Second).Unix()},
			{TargetPeerID: "peer-stable", ContentID: "sha256-demo", LastPath: "udp", LastAt: now.Add(-3 * time.Second).Unix()},
			{TargetPeerID: "peer-recover", ContentID: "sha256-demo", LastPath: "udp", LastAt: now.Add(-3 * time.Second).Unix()},
		},
		UDPDecisions: []tracker.UDPDecisionStatus{
			{TargetPeerID: "peer-low", ContentID: "sha256-demo", BurstProfile: "aggressive", LastStage: "probe", ReportCount: 3, LastReportedAt: now.Add(-2 * time.Second).Unix()},
			{TargetPeerID: "peer-stable", ContentID: "sha256-demo", BurstProfile: "warm", LastStage: "piece", ReportCount: 2, LastReportedAt: now.Add(-2 * time.Second).Unix()},
			{TargetPeerID: "peer-recover", ContentID: "sha256-demo", BurstProfile: "default", LastStage: "piece", ReportCount: 2, LastReportedAt: now.Add(-2 * time.Second).Unix()},
		},
		Swarms: []tracker.SwarmStatus{
			{
				ContentID: "sha256-demo",
				Peers: []tracker.PeerRecord{
					{PeerID: "peer-low", UDPAddrs: []string{"198.51.100.10:9003"}},
					{PeerID: "peer-stable", UDPAddrs: []string{"198.51.100.11:9003"}},
					{PeerID: "peer-recover", UDPAddrs: []string{"198.51.100.12:9003"}},
				},
			},
		},
	}

	biases := buildTrackerUDPPeerBiases(status, "sha256-demo", now)
	if got := biases["peer-low"]; math.Abs(got-(-0.68)) > 1e-9 {
		t.Fatalf("expected peer-low combined bias -0.68, got %.2f", got)
	}
	if got := biases["peer-stable"]; math.Abs(got-0.39) > 1e-9 {
		t.Fatalf("expected peer-stable combined bias 0.39, got %.2f", got)
	}
	if got := biases["peer-recover"]; math.Abs(got-(-0.10)) > 1e-9 {
		t.Fatalf("expected peer-recover combined bias -0.10, got %.2f", got)
	}
}

func TestBuildTrackerUDPPeerBiasesIncludesUDPSessionHealthBias(t *testing.T) {
	now := time.Unix(780, 0)
	status := tracker.StatusResponse{
		UDPSessionHealths: []tracker.UDPSessionHealthStatus{
			{
				TargetPeerID:              "peer-active",
				ContentID:                 "sha256-demo",
				State:                     "active",
				HealthScore:               0.62,
				LastStage:                 "piece",
				RecommendedChunkWindow:    6,
				RecommendedRoundTimeoutMs: 820,
				RecommendedAttemptBudget:  5,
				LastReportedAt:            now.Add(-5 * time.Second).Unix(),
			},
			{
				TargetPeerID:   "peer-cooling",
				ContentID:      "sha256-demo",
				State:          "cooling",
				HealthScore:    -0.30,
				LastStage:      "probe",
				LastErrorKind:  "udp_timeout",
				LastReportedAt: now.Add(-4 * time.Second).Unix(),
			},
		},
		Swarms: []tracker.SwarmStatus{
			{
				ContentID: "sha256-demo",
				Peers: []tracker.PeerRecord{
					{PeerID: "peer-active", UDPAddrs: []string{"198.51.100.20:9003"}},
					{PeerID: "peer-cooling", UDPAddrs: []string{"198.51.100.21:9003"}},
				},
			},
		},
	}

	biases := buildTrackerUDPPeerBiases(status, "sha256-demo", now)
	if got := biases["peer-active"]; math.Abs(got-0.18) > 1e-9 {
		t.Fatalf("expected active session bias 0.18, got %.2f", got)
	}
	if got := biases["peer-cooling"]; math.Abs(got-(-0.14)) > 1e-9 {
		t.Fatalf("expected cooling session bias -0.14, got %.2f", got)
	}
}

func TestRequesterSideBurstPunchTargetsPrefersSTUNObservedAndDeduplicates(t *testing.T) {
	peer := tracker.PeerRecord{
		PeerID:            "peer-a",
		ObservedUDPAddr:   "198.51.100.10:9003",
		ObservedUDPSource: "stun",
		UDPAddrs: []string{
			"198.51.100.10:9003",
			"198.51.100.11:9003",
			"198.51.100.11:9003",
			"",
			"198.51.100.12:9003",
		},
	}

	got := requesterSideBurstPunchTargets(peer, "198.51.100.12:9003")
	want := []string{
		"198.51.100.10:9003",
		"198.51.100.11:9003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected burst punch targets %v, got %v", want, got)
	}
}

func TestRequesterSideBurstPunchTargetsKeepsDeclaredFirstWithoutSTUNObservation(t *testing.T) {
	peer := tracker.PeerRecord{
		PeerID:          "peer-a",
		ObservedUDPAddr: "198.51.100.10:9003",
		UDPAddrs: []string{
			"198.51.100.11:9003",
			"198.51.100.12:9003",
		},
	}

	got := requesterSideBurstPunchTargets(peer, "198.51.100.99:9003")
	want := []string{
		"198.51.100.11:9003",
		"198.51.100.12:9003",
		"198.51.100.10:9003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected non-stun observed target to trail declared addrs, got %v", got)
	}
}

func TestRequesterSideBurstPunchTargetsFallsBackToDeclaredUDPAddrs(t *testing.T) {
	peer := tracker.PeerRecord{
		PeerID: "peer-b",
		UDPAddrs: []string{
			"198.51.100.20:9003",
			"198.51.100.21:9003",
		},
	}

	got := requesterSideBurstPunchTargets(peer, "198.51.100.99:9003")
	want := []string{
		"198.51.100.20:9003",
		"198.51.100.21:9003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected declared udp addrs %v, got %v", want, got)
	}
}

func TestResponderSideBurstPunchTargetsPrefersSTUNObservedRequesterAddr(t *testing.T) {
	request := tracker.UDPProbeTask{
		RequesterUDPAddr:  "198.51.100.20:9003",
		ObservedUDPAddr:   "203.0.113.20:40123",
		ObservedUDPSource: "stun",
	}

	got := responderSideBurstPunchTargets(request)
	want := []string{"203.0.113.20:40123", "198.51.100.20:9003"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected stun-observed requester addr first, got %v", got)
	}
}

func TestResponderSideBurstPunchTargetsPrefersRequesterAddrWithoutSTUN(t *testing.T) {
	request := tracker.UDPProbeTask{
		RequesterUDPAddr: "198.51.100.20:9003",
		ObservedUDPAddr:  "203.0.113.20:40123",
	}

	got := responderSideBurstPunchTargets(request)
	want := []string{"198.51.100.20:9003", "203.0.113.20:40123"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected requester-declared addr first without stun source, got %v", got)
	}
}

func TestAdaptiveRequesterBurstPhasesUsesWarmProfileForRecentObservedPath(t *testing.T) {
	now := time.Unix(800, 0)
	p2pnet.RememberObservedUDPPeer("sha256-demo", "peer-a", "198.51.100.1:9003", now.Add(-5*time.Second))
	peer := tracker.PeerRecord{PeerID: "peer-a", UDPAddrs: []string{"198.51.100.1:9003"}}

	got := adaptiveRequesterBurstPhases(peer, "sha256-demo", tracker.StatusResponse{}, now)
	want := warmUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected warm burst phases %v, got %v", want, got)
	}
}

func TestAdaptiveRequesterBurstPhasesUsesAggressiveProfileForRecentFallback(t *testing.T) {
	now := time.Unix(900, 0)
	peer := tracker.PeerRecord{PeerID: "peer-b", UDPAddrs: []string{"198.51.100.2:9003"}}
	status := tracker.StatusResponse{
		UDPProbeResults: []tracker.UDPProbeResultStatus{
			{
				TargetPeerID:  "peer-b",
				LastFailureAt: now.Add(-6 * time.Second).Unix(),
				LastErrorKind: "udp_timeout",
			},
		},
		PeerTransferPaths: []tracker.PeerTransferPathStatus{
			{
				TargetPeerID: "peer-b",
				ContentID:    "sha256-demo",
				LastPath:     "tcp",
				LastAt:       now.Add(-5 * time.Second).Unix(),
				TCPCount:     3,
			},
		},
		UDPKeepaliveResults: []tracker.UDPKeepaliveStatus{
			{
				TargetPeerID:  "udp://198.51.100.2:9003",
				ContentID:     "sha256-demo",
				LastFailureAt: now.Add(-4 * time.Second).Unix(),
				LastErrorKind: "udp_timeout",
				FailureCount:  2,
			},
		},
	}

	got := adaptiveRequesterBurstPhases(peer, "sha256-demo", status, now)
	want := aggressiveUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected aggressive burst phases %v, got %v", want, got)
	}
}

func TestAdaptiveRequesterBurstPhasesUsesDefaultProfileWithoutRecentSignals(t *testing.T) {
	now := time.Unix(1000, 0)
	peer := tracker.PeerRecord{PeerID: "peer-c", UDPAddrs: []string{"198.51.100.3:9003"}}

	got := adaptiveRequesterBurstPhases(peer, "sha256-demo", tracker.StatusResponse{}, now)
	want := defaultUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected default burst phases %v, got %v", want, got)
	}
}

func TestAdaptiveRequesterBurstPhasesUsesTrackerWarmBurstProfile(t *testing.T) {
	now := time.Unix(1050, 0)
	peer := tracker.PeerRecord{PeerID: "peer-tracker-warm", UDPAddrs: []string{"198.51.100.30:9003"}}
	status := tracker.StatusResponse{
		UDPBurstProfiles: []tracker.UDPBurstProfileStatus{
			{
				TargetPeerID:   "peer-tracker-warm",
				ContentID:      "sha256-demo",
				Profile:        "warm",
				LastOutcome:    "success",
				LastReportedAt: now.Add(-8 * time.Second).Unix(),
			},
		},
	}

	got := adaptiveRequesterBurstPhases(peer, "sha256-demo", status, now)
	want := warmUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected tracker-driven warm burst phases %v, got %v", want, got)
	}
}

func TestAdaptiveRequesterBurstPhasesUsesTrackerAggressiveBurstProfile(t *testing.T) {
	now := time.Unix(1075, 0)
	peer := tracker.PeerRecord{PeerID: "peer-tracker-aggressive", UDPAddrs: []string{"198.51.100.31:9003"}}
	status := tracker.StatusResponse{
		UDPBurstProfiles: []tracker.UDPBurstProfileStatus{
			{
				TargetPeerID:   "peer-tracker-aggressive",
				ContentID:      "sha256-demo",
				Profile:        "aggressive",
				LastOutcome:    "failure",
				FailureCount:   3,
				LastReportedAt: now.Add(-6 * time.Second).Unix(),
			},
		},
	}

	got := adaptiveRequesterBurstPhases(peer, "sha256-demo", status, now)
	want := aggressiveUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected tracker-driven aggressive burst phases %v, got %v", want, got)
	}
}

func TestAdaptiveResponderBurstPhasesUsesWarmProfileForRecentRequesterObservation(t *testing.T) {
	now := time.Unix(1100, 0)
	p2pnet.RememberObservedUDPPeer("sha256-demo", "peer-d", "198.51.100.4:9003", now.Add(-4*time.Second))
	request := tracker.UDPProbeTask{
		ContentID:       "sha256-demo",
		RequesterPeerID: "peer-d",
		ObservedUDPAddr: "198.51.100.4:9003",
	}

	got := adaptiveResponderBurstPhases(request, now)
	want := warmUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected warm responder burst phases %v, got %v", want, got)
	}
}

func TestAdaptiveResponderBurstPhasesUsesAggressiveProfileWithoutObservedAddr(t *testing.T) {
	now := time.Unix(1200, 0)
	request := tracker.UDPProbeTask{
		ContentID:       "sha256-demo",
		RequesterPeerID: "peer-e",
	}

	got := adaptiveResponderBurstPhases(request, now)
	want := aggressiveUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected aggressive responder burst phases %v, got %v", want, got)
	}
}

func TestLearnedUDPBurstPhasesReusesRecentSuccessfulProfile(t *testing.T) {
	now := time.Unix(1300, 0)
	recordUDPBurstOutcome("sha256-demo", "peer-f", "warm", "probe", true, now.Add(-5*time.Second))

	got, ok := learnedUDPBurstPhases("sha256-demo", "peer-f", now)
	if !ok {
		t.Fatal("expected learned burst phases to exist after recent success")
	}
	want := warmUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected learned warm burst phases %v, got %v", want, got)
	}
}

func TestLearnedUDPBurstPhasesEscalatesAfterRepeatedRecentFailures(t *testing.T) {
	now := time.Unix(1400, 0)
	recordUDPBurstOutcome("sha256-demo", "peer-g", "default", "probe", false, now.Add(-8*time.Second))
	recordUDPBurstOutcome("sha256-demo", "peer-g", "default", "probe", false, now.Add(-3*time.Second))

	got, ok := learnedUDPBurstPhases("sha256-demo", "peer-g", now)
	if !ok {
		t.Fatal("expected learned burst phases to exist after repeated recent failures")
	}
	want := aggressiveUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected escalated aggressive burst phases %v, got %v", want, got)
	}
}

func TestCurrentUDPBurstProfilesReturnsPerPeerSnapshot(t *testing.T) {
	now := time.Unix(1500, 0)
	recordUDPBurstOutcome("sha256-demo", "peer-h", "warm", "have", true, now.Add(-6*time.Second))
	recordUDPBurstOutcome("sha256-demo", "peer-i", "default", "piece", false, now.Add(-4*time.Second))

	got := currentUDPBurstProfiles("sha256-demo", now)
	if len(got) < 2 {
		t.Fatalf("expected at least two udp burst profiles, got %+v", got)
	}

	foundWarm := false
	foundDefault := false
	for _, profile := range got {
		switch profile.PeerID {
		case "peer-h":
			foundWarm = profile.Profile == "warm" && profile.LastSuccessAt != "" && profile.LastStage == "have"
		case "peer-i":
			foundDefault = profile.Profile == "default" && profile.LastFailureAt != "" && profile.FailureCount >= 1 && profile.LastStage == "piece"
		}
	}
	if !foundWarm || !foundDefault {
		t.Fatalf("expected peer-h warm and peer-i default profiles in snapshot, got %+v", got)
	}
}

func TestLearnedUDPBurstPhasesEscalatesPieceFailuresMoreGradually(t *testing.T) {
	now := time.Unix(1600, 0)
	recordUDPBurstOutcome("sha256-demo", "peer-piece", "warm", "piece", false, now.Add(-7*time.Second))
	recordUDPBurstOutcome("sha256-demo", "peer-piece", "warm", "piece", false, now.Add(-2*time.Second))

	got, ok := learnedUDPBurstPhases("sha256-demo", "peer-piece", now)
	if !ok {
		t.Fatal("expected learned burst phases to exist after repeated recent piece failures")
	}
	want := defaultUDPBurstPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected piece failures to step warm profile up to default phases %v, got %v", want, got)
	}
}
