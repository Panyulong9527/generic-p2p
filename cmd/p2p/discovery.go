package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/logging"
	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/scheduler"
	"generic-p2p/internal/tracker"
)

type udpPeerPreference struct {
	observedAt   time.Time
	trackerBias  float64
	burstProfile string
	decisionRisk string
}

type udpBurstProfileStats struct {
	LastProfile      string
	LastSuccessAt    time.Time
	LastSuccessStage string
	LastFailureAt    time.Time
	LastFailureStage string
	FailureCount     int
}

var udpBurstLearningState = struct {
	mu      sync.Mutex
	entries map[string]udpBurstProfileStats
}{
	entries: make(map[string]udpBurstProfileStats),
}

func discoverTrackerPeers(logger *logging.Logger, contentID string, trackerURL string, selfListenAddr string) ([]string, error) {
	client := tracker.NewClient(trackerURL)
	peers, err := client.GetPeers(context.Background(), contentID)
	if err != nil {
		return nil, fmt.Errorf("discover tracker peers: %w", err)
	}

	addrs := make([]string, 0, len(peers))
	for _, peer := range peers {
		for _, addr := range peer.Addrs {
			if addr == "" || addr == selfListenAddr {
				continue
			}
			logger.Info("tracker_peer_discovered",
				"contentId", contentID,
				"peerId", peer.PeerID,
				"listen", addr,
				"ranges", peer.HaveRanges,
			)
			addrs = append(addrs, addr)
		}
		if peer.ObservedAddr != "" && peer.ObservedAddr != selfListenAddr {
			logger.Info("tracker_observed_peer_discovered",
				"contentId", contentID,
				"peerId", peer.PeerID,
				"listen", peer.ObservedAddr,
				"ranges", peer.HaveRanges,
			)
			addrs = append(addrs, peer.ObservedAddr)
		}
	}
	return addrs, nil
}

func discoverTrackerUDPPeers(logger *logging.Logger, contentID string, trackerURL string, selfPeerID string, selfUDPListenAddr string, udpProbeRequests *udpProbeRequestCache, trackerStatus *trackerStatusCache) ([]string, map[string]udpPeerPreference, error) {
	client := tracker.NewClient(trackerURL)
	peers, err := client.GetPeers(context.Background(), contentID)
	if err != nil {
		return nil, nil, fmt.Errorf("discover tracker udp peers: %w", err)
	}

	now := time.Now()
	status, _ := loadTrackerStatus(client, trackerURL, trackerStatus, now)
	probeBiases := buildTrackerUDPPeerBiases(status, contentID, now)
	addrs := make([]string, 0, len(peers))
	preferences := make(map[string]udpPeerPreference)
	for _, peer := range peers {
		now := time.Now()
		probeResult, transferPath, _, burstProfile := trackerPeerUDPState(status, contentID, peer)
		decisionRisk := trackerUDPDecisionRiskKind(peer, probeResult, transferPath, trackerPeerUDPDecision(status, contentID, peer))
		probeRequestKey := contentID + "|" + selfPeerID + "|" + selfUDPListenAddr + "|" + peer.PeerID
		if selfPeerID != "" && selfUDPListenAddr != "" && peer.PeerID != "" && peer.PeerID != selfPeerID && udpProbeRequests.ShouldRequest(probeRequestKey, now) {
			if err := client.RequestUDPProbe(context.Background(), contentID, selfPeerID, selfUDPListenAddr, peer.PeerID); err != nil {
				logger.Error("tracker_udp_probe_request_failed",
					"contentId", contentID,
					"peerId", peer.PeerID,
					"error", err.Error(),
				)
			} else {
				maybeRequesterSideBurstPunch(logger, contentID, peer, selfUDPListenAddr, adaptiveRequesterBurstPhases(peer, contentID, status, now))
			}
		}
		if observedAddr, seenAt, ok := p2pnet.ObservedUDPPeer(contentID, peer.PeerID, 30*time.Second, now); ok && observedAddr != selfUDPListenAddr {
			logger.Info("local_observed_udp_peer_discovered",
				"contentId", contentID,
				"peerId", peer.PeerID,
				"listen", observedAddr,
				"ranges", peer.HaveRanges,
			)
			addrs = append(addrs, observedAddr)
			pref := preferences[observedAddr]
			pref.observedAt = seenAt
			pref.trackerBias = maxFloat64(pref.trackerBias, probeBiases[peer.PeerID])
			pref.burstProfile = strings.TrimSpace(burstProfile.Profile)
			pref.decisionRisk = decisionRisk
			preferences[observedAddr] = pref
			maybeKeepAliveUDPPath(logger, contentID, peer.PeerID, trackerURL, selfUDPListenAddr, observedAddr, now)
		}
		for _, addr := range peer.UDPAddrs {
			if addr == "" || addr == selfUDPListenAddr {
				continue
			}
			logger.Info("tracker_udp_peer_discovered",
				"contentId", contentID,
				"peerId", peer.PeerID,
				"listen", addr,
				"ranges", peer.HaveRanges,
			)
			addrs = append(addrs, addr)
			pref := preferences[addr]
			pref.trackerBias = maxFloat64(pref.trackerBias, probeBiases[peer.PeerID])
			pref.burstProfile = strings.TrimSpace(burstProfile.Profile)
			pref.decisionRisk = decisionRisk
			preferences[addr] = pref
		}
		if peer.ObservedUDPAddr != "" && peer.ObservedUDPAddr != selfUDPListenAddr {
			logger.Info("tracker_observed_udp_peer_discovered",
				"contentId", contentID,
				"peerId", peer.PeerID,
				"listen", peer.ObservedUDPAddr,
				"ranges", peer.HaveRanges,
			)
			addrs = append(addrs, peer.ObservedUDPAddr)
			pref := preferences[peer.ObservedUDPAddr]
			pref.trackerBias = maxFloat64(pref.trackerBias, probeBiases[peer.PeerID])
			pref.burstProfile = strings.TrimSpace(burstProfile.Profile)
			pref.decisionRisk = decisionRisk
			preferences[peer.ObservedUDPAddr] = pref
			maybeKeepAliveUDPPath(logger, contentID, peer.PeerID, trackerURL, selfUDPListenAddr, peer.ObservedUDPAddr, now)
		}
	}
	return addrs, preferences, nil
}

func maybeRequesterSideBurstPunch(logger *logging.Logger, contentID string, peer tracker.PeerRecord, selfUDPListenAddr string, phases []p2pnet.UDPBurstPhase) {
	if strings.TrimSpace(selfUDPListenAddr) == "" || strings.TrimSpace(peer.PeerID) == "" {
		return
	}
	targets := requesterSideBurstPunchTargets(peer, selfUDPListenAddr)
	if len(targets) == 0 {
		return
	}
	go func() {
		profile := udpBurstProfileName(phases)
		for _, target := range targets {
			if err := p2pnet.NewUDPClient(target, 1500*time.Millisecond).
				WithLocalAddr(selfUDPListenAddr).
				ProbeMultiBurstForPeer(contentID, peer.PeerID, phases); err != nil {
				recordUDPBurstOutcome(contentID, peer.PeerID, profile, "probe", false, time.Now())
				logger.Info("tracker_udp_requester_burst_probe_failed",
					"contentId", contentID,
					"peerId", peer.PeerID,
					"profile", profile,
					"remote", target,
					"error", err.Error(),
				)
				continue
			}
			recordUDPBurstOutcome(contentID, peer.PeerID, profile, "probe", true, time.Now())
			logger.Info("tracker_udp_requester_burst_probe_sent",
				"contentId", contentID,
				"peerId", peer.PeerID,
				"profile", profile,
				"remote", target,
			)
		}
	}()
}

func requesterSideBurstPunchTargets(peer tracker.PeerRecord, selfUDPListenAddr string) []string {
	targets := make([]string, 0, len(peer.UDPAddrs)+1)
	appendTarget := func(addr string) {
		addr = strings.TrimSpace(addr)
		if addr == "" || addr == selfUDPListenAddr {
			return
		}
		for _, existing := range targets {
			if existing == addr {
				return
			}
		}
		targets = append(targets, addr)
	}

	appendTarget(peer.ObservedUDPAddr)
	for _, addr := range peer.UDPAddrs {
		appendTarget(addr)
	}
	return targets
}

func loadTrackerStatus(client *tracker.Client, trackerURL string, statusCache *trackerStatusCache, now time.Time) (tracker.StatusResponse, bool) {
	if statusCache != nil {
		if cached, ok := statusCache.Load(trackerURL, now); ok {
			return cached, true
		}
	}
	status, err := client.GetStatus(context.Background())
	if err != nil {
		return tracker.StatusResponse{}, false
	}
	if statusCache != nil {
		statusCache.Store(trackerURL, status, now)
	}
	return status, true
}

func defaultUDPBurstPhases() []p2pnet.UDPBurstPhase {
	return []p2pnet.UDPBurstPhase{
		{Attempts: 3, Gap: 80 * time.Millisecond},
		{Attempts: 2, Gap: 250 * time.Millisecond},
	}
}

func warmUDPBurstPhases() []p2pnet.UDPBurstPhase {
	return []p2pnet.UDPBurstPhase{
		{Attempts: 2, Gap: 70 * time.Millisecond},
		{Attempts: 1, Gap: 180 * time.Millisecond},
	}
}

func aggressiveUDPBurstPhases() []p2pnet.UDPBurstPhase {
	return []p2pnet.UDPBurstPhase{
		{Attempts: 4, Gap: 60 * time.Millisecond},
		{Attempts: 3, Gap: 180 * time.Millisecond},
		{Attempts: 2, Gap: 320 * time.Millisecond},
	}
}

func adaptiveRequesterBurstPhases(peer tracker.PeerRecord, contentID string, status tracker.StatusResponse, now time.Time) []p2pnet.UDPBurstPhase {
	if learned, ok := learnedUDPBurstPhases(contentID, peer.PeerID, now); ok {
		return learned
	}
	probeResult, transferPath, keepaliveResult, burstProfile := trackerPeerUDPState(status, contentID, peer)
	if trackerBurstProfileNeedsAggressive(burstProfile, now) {
		return aggressiveUDPBurstPhases()
	}
	if trackerBurstProfileIsWarm(burstProfile, now) {
		return warmUDPBurstPhases()
	}
	if trackerPeerNeedsAggressiveBurst(peer, probeResult, transferPath, keepaliveResult, now) {
		return aggressiveUDPBurstPhases()
	}
	if requesterHasWarmUDPPath(peer.PeerID, contentID, probeResult, transferPath, keepaliveResult, now) {
		return warmUDPBurstPhases()
	}
	return defaultUDPBurstPhases()
}

func adaptiveResponderBurstPhases(request tracker.UDPProbeTask, now time.Time) []p2pnet.UDPBurstPhase {
	if learned, ok := learnedUDPBurstPhases(request.ContentID, request.RequesterPeerID, now); ok {
		return learned
	}
	if requesterAddr, _, ok := p2pnet.ObservedUDPPeer(request.ContentID, request.RequesterPeerID, 12*time.Second, now); ok && strings.TrimSpace(requesterAddr) != "" {
		return warmUDPBurstPhases()
	}
	if strings.TrimSpace(request.ObservedUDPAddr) == "" {
		return aggressiveUDPBurstPhases()
	}
	return defaultUDPBurstPhases()
}

func requesterHasWarmUDPPath(peerID string, contentID string, probeResult tracker.UDPProbeResultStatus, transferPath tracker.PeerTransferPathStatus, keepaliveResult tracker.UDPKeepaliveStatus, now time.Time) bool {
	if peerID != "" {
		if _, _, ok := p2pnet.ObservedUDPPeer(contentID, peerID, 12*time.Second, now); ok {
			return true
		}
	}
	if probeResult.LastSuccessAt > probeResult.LastFailureAt && probeResult.LastSuccessAt > 0 && now.Sub(time.Unix(probeResult.LastSuccessAt, 0)) <= 12*time.Second {
		return true
	}
	if keepaliveResult.LastSuccessAt > keepaliveResult.LastFailureAt && keepaliveResult.LastSuccessAt > 0 && now.Sub(time.Unix(keepaliveResult.LastSuccessAt, 0)) <= 12*time.Second {
		return true
	}
	if strings.TrimSpace(strings.ToLower(transferPath.LastPath)) == "udp" && transferPath.LastAt > 0 && now.Sub(time.Unix(transferPath.LastAt, 0)) <= 12*time.Second {
		return true
	}
	return false
}

func trackerPeerNeedsAggressiveBurst(peer tracker.PeerRecord, probeResult tracker.UDPProbeResultStatus, transferPath tracker.PeerTransferPathStatus, keepaliveResult tracker.UDPKeepaliveStatus, now time.Time) bool {
	if probeResult.LastFailureAt > probeResult.LastSuccessAt && probeResult.LastFailureAt > 0 && now.Sub(time.Unix(probeResult.LastFailureAt, 0)) <= 20*time.Second {
		return true
	}
	if keepaliveResult.LastFailureAt > keepaliveResult.LastSuccessAt && keepaliveResult.LastFailureAt > 0 && now.Sub(time.Unix(keepaliveResult.LastFailureAt, 0)) <= 20*time.Second {
		return true
	}
	if trackerUDPFallbackBias(transferPath, keepaliveResult, now) < 0 {
		return true
	}
	if strings.TrimSpace(strings.ToLower(transferPath.LastPath)) == "tcp" && transferPath.LastAt > 0 {
		probeAdvice := discoveryTrackerPeerRouteAdvice(peer, probeResult)
		if discoveryTrackerRouteDrift(probeAdvice, transferPath.LastPath) == "udp_miss" && now.Sub(time.Unix(transferPath.LastAt, 0)) <= 20*time.Second {
			return true
		}
	}
	return false
}

func trackerPeerUDPState(status tracker.StatusResponse, contentID string, peer tracker.PeerRecord) (tracker.UDPProbeResultStatus, tracker.PeerTransferPathStatus, tracker.UDPKeepaliveStatus, tracker.UDPBurstProfileStatus) {
	var probeResult tracker.UDPProbeResultStatus
	for _, item := range status.UDPProbeResults {
		if item.TargetPeerID == peer.PeerID {
			probeResult = item
			break
		}
	}
	var transferPath tracker.PeerTransferPathStatus
	for _, item := range status.PeerTransferPaths {
		if item.TargetPeerID == peer.PeerID && (strings.TrimSpace(item.ContentID) == "" || item.ContentID == contentID) {
			transferPath = item
			break
		}
	}
	keepaliveResults := make(map[string]tracker.UDPKeepaliveStatus, len(status.UDPKeepaliveResults))
	for _, item := range status.UDPKeepaliveResults {
		if strings.TrimSpace(item.ContentID) != "" && item.ContentID != contentID {
			continue
		}
		keepaliveResults[item.TargetPeerID] = item
	}
	var burstProfile tracker.UDPBurstProfileStatus
	for _, item := range status.UDPBurstProfiles {
		if item.TargetPeerID != peer.PeerID {
			continue
		}
		if strings.TrimSpace(item.ContentID) != "" && item.ContentID != contentID {
			continue
		}
		burstProfile = item
		break
	}
	return probeResult, transferPath, peerKeepaliveResult(peer, keepaliveResults), burstProfile
}

func trackerPeerUDPDecision(status tracker.StatusResponse, contentID string, peer tracker.PeerRecord) tracker.UDPDecisionStatus {
	for _, item := range status.UDPDecisions {
		if item.TargetPeerID != peer.PeerID {
			continue
		}
		if strings.TrimSpace(item.ContentID) != "" && item.ContentID != contentID {
			continue
		}
		return item
	}
	return tracker.UDPDecisionStatus{}
}

func udpBurstProfileName(phases []p2pnet.UDPBurstPhase) string {
	switch {
	case reflectBurstPhasesEqual(phases, warmUDPBurstPhases()):
		return "warm"
	case reflectBurstPhasesEqual(phases, aggressiveUDPBurstPhases()):
		return "aggressive"
	default:
		return "default"
	}
}

func learnedUDPBurstPhases(contentID string, peerID string, now time.Time) ([]p2pnet.UDPBurstPhase, bool) {
	stats, ok := udpBurstStats(contentID, peerID, now)
	if !ok {
		return nil, false
	}
	if !stats.LastSuccessAt.IsZero() && now.Sub(stats.LastSuccessAt) <= 30*time.Second {
		return phasesForBurstProfile(stats.LastProfile), true
	}
	if stats.FailureCount >= 2 && !stats.LastFailureAt.IsZero() && now.Sub(stats.LastFailureAt) <= 20*time.Second {
		switch nextUDPBurstProfileAfterFailure(stats) {
		case "warm":
			return warmUDPBurstPhases(), true
		case "default":
			return defaultUDPBurstPhases(), true
		case "aggressive":
			return aggressiveUDPBurstPhases(), true
		}
	}
	return nil, false
}

func recordUDPBurstOutcome(contentID string, peerID string, profile string, stage string, success bool, now time.Time) {
	if strings.TrimSpace(contentID) == "" || strings.TrimSpace(peerID) == "" || strings.TrimSpace(profile) == "" {
		return
	}
	key := contentID + "|" + peerID
	udpBurstLearningState.mu.Lock()
	defer udpBurstLearningState.mu.Unlock()
	stats := udpBurstLearningState.entries[key]
	previousProfile := stats.LastProfile
	stats.LastProfile = profile
	if success {
		stats.LastSuccessAt = now
		stats.LastSuccessStage = strings.TrimSpace(stage)
		stats.FailureCount = 0
	} else {
		stats.LastFailureAt = now
		stats.LastFailureStage = strings.TrimSpace(stage)
		if previousProfile == profile {
			stats.FailureCount++
		} else {
			stats.FailureCount = 1
		}
	}
	udpBurstLearningState.entries[key] = stats
}

func udpBurstStats(contentID string, peerID string, now time.Time) (udpBurstProfileStats, bool) {
	if strings.TrimSpace(contentID) == "" || strings.TrimSpace(peerID) == "" {
		return udpBurstProfileStats{}, false
	}
	key := contentID + "|" + peerID
	udpBurstLearningState.mu.Lock()
	defer udpBurstLearningState.mu.Unlock()
	stats, ok := udpBurstLearningState.entries[key]
	if !ok {
		return udpBurstProfileStats{}, false
	}
	if now.Sub(maxTime(stats.LastSuccessAt, stats.LastFailureAt)) > time.Minute {
		delete(udpBurstLearningState.entries, key)
		return udpBurstProfileStats{}, false
	}
	return stats, true
}

func currentUDPBurstProfiles(contentID string, now time.Time) []core.UDPBurstProfileStatus {
	if strings.TrimSpace(contentID) == "" {
		return nil
	}
	prefix := contentID + "|"
	udpBurstLearningState.mu.Lock()
	defer udpBurstLearningState.mu.Unlock()

	profiles := make([]core.UDPBurstProfileStatus, 0)
	for key, stats := range udpBurstLearningState.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if now.Sub(maxTime(stats.LastSuccessAt, stats.LastFailureAt)) > time.Minute {
			delete(udpBurstLearningState.entries, key)
			continue
		}
		peerID := strings.TrimPrefix(key, prefix)
		item := core.UDPBurstProfileStatus{
			PeerID:       peerID,
			Profile:      stats.LastProfile,
			LastStage:    currentUDPBurstStage(stats),
			FailureCount: stats.FailureCount,
		}
		if !stats.LastSuccessAt.IsZero() {
			item.LastSuccessAt = stats.LastSuccessAt.Format(time.RFC3339)
		}
		if !stats.LastFailureAt.IsZero() {
			item.LastFailureAt = stats.LastFailureAt.Format(time.RFC3339)
		}
		profiles = append(profiles, item)
	}
	sort.Slice(profiles, func(i, j int) bool {
		if profiles[i].Profile != profiles[j].Profile {
			return profiles[i].Profile < profiles[j].Profile
		}
		return profiles[i].PeerID < profiles[j].PeerID
	})
	return profiles
}

func nextUDPBurstProfileAfterFailure(stats udpBurstProfileStats) string {
	switch strings.TrimSpace(stats.LastFailureStage) {
	case "piece":
		switch strings.TrimSpace(stats.LastProfile) {
		case "warm":
			return "default"
		case "default":
			if stats.FailureCount >= 3 {
				return "aggressive"
			}
			return "default"
		default:
			return "aggressive"
		}
	case "probe", "have":
		return "aggressive"
	default:
		return "aggressive"
	}
}

func currentUDPBurstStage(stats udpBurstProfileStats) string {
	if stats.LastFailureAt.After(stats.LastSuccessAt) {
		return strings.TrimSpace(stats.LastFailureStage)
	}
	return strings.TrimSpace(stats.LastSuccessStage)
}

func trackerBurstProfileNeedsAggressive(item tracker.UDPBurstProfileStatus, now time.Time) bool {
	if strings.TrimSpace(item.Profile) != "aggressive" || item.LastReportedAt == 0 {
		return false
	}
	if now.Sub(time.Unix(item.LastReportedAt, 0)) > 45*time.Second {
		return false
	}
	return true
}

func trackerBurstProfileIsWarm(item tracker.UDPBurstProfileStatus, now time.Time) bool {
	if strings.TrimSpace(item.Profile) != "warm" || item.LastReportedAt == 0 {
		return false
	}
	if strings.TrimSpace(item.LastOutcome) == "failure" {
		return false
	}
	return now.Sub(time.Unix(item.LastReportedAt, 0)) <= 45*time.Second
}

func trackerBurstProfileBias(item tracker.UDPBurstProfileStatus, now time.Time) float64 {
	if item.LastReportedAt == 0 {
		return 0
	}
	if now.Sub(time.Unix(item.LastReportedAt, 0)) > 45*time.Second {
		return 0
	}
	switch strings.TrimSpace(item.Profile) {
	case "warm":
		return 0.08 + trackerBurstProfileStageBias(item)
	case "aggressive":
		if strings.TrimSpace(item.LastOutcome) == "failure" {
			return -0.08 + trackerBurstProfileStageBias(item)
		}
		return -0.04 + trackerBurstProfileStageBias(item)
	default:
		return 0
	}
}

func trackerBurstProfileStageBias(item tracker.UDPBurstProfileStatus) float64 {
	stage := strings.TrimSpace(item.LastStage)
	outcome := strings.TrimSpace(item.LastOutcome)

	switch outcome {
	case "success":
		switch stage {
		case "piece":
			return 0.06
		case "have":
			return 0.03
		default:
			return 0
		}
	case "failure":
		switch stage {
		case "probe":
			return -0.10
		case "have":
			return -0.06
		case "piece":
			return -0.03
		default:
			return 0
		}
	default:
		return 0
	}
}

func phasesForBurstProfile(profile string) []p2pnet.UDPBurstPhase {
	switch strings.TrimSpace(profile) {
	case "warm":
		return warmUDPBurstPhases()
	case "aggressive":
		return aggressiveUDPBurstPhases()
	default:
		return defaultUDPBurstPhases()
	}
}

func reflectBurstPhasesEqual(left []p2pnet.UDPBurstPhase, right []p2pnet.UDPBurstPhase) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func maxTime(left time.Time, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func maybeKeepAliveUDPPath(logger *logging.Logger, contentID string, peerID string, trackerURL string, selfUDPListenAddr string, remoteAddr string, now time.Time) {
	if !p2pnet.ShouldKeepAliveObservedUDPPeer(selfUDPListenAddr, contentID, peerID, remoteAddr, 8*time.Second, now) {
		return
	}
	go func() {
		if err := p2pnet.NewUDPClient(remoteAddr, 1500*time.Millisecond).WithLocalAddr(selfUDPListenAddr).ProbeForPeer(contentID, peerID); err != nil {
			reportTrackerUDPKeepalive(logger, trackerURL, "udp://"+remoteAddr, contentID, false, udpDiscoveryErrorKind(err))
			logger.Info("udp_keepalive_failed",
				"contentId", contentID,
				"peerId", peerID,
				"remote", remoteAddr,
				"error", err.Error(),
			)
			return
		}
		reportTrackerUDPKeepalive(logger, trackerURL, "udp://"+remoteAddr, contentID, true, "")
		logger.Info("udp_keepalive_sent",
			"contentId", contentID,
			"peerId", peerID,
			"remote", remoteAddr,
		)
	}()
}

func collectPeerAddrs(peerAddr string, peerList string) []string {
	unique := make(map[string]bool)
	ordered := make([]string, 0)

	appendAddr := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || unique[value] {
			return
		}
		unique[value] = true
		ordered = append(ordered, value)
	}

	appendAddr(peerAddr)
	for _, part := range strings.Split(peerList, ",") {
		appendAddr(part)
	}
	return ordered
}

func appendUnique(base []string, values ...string) []string {
	seen := make(map[string]bool, len(base))
	for _, value := range base {
		seen[value] = true
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		base = append(base, value)
		seen[value] = true
	}
	return base
}

func collectDynamicPeerCandidates(logger *logging.Logger, options peerDiscoveryOptions, peerHealth *peerHealthState, discoveryCache *peerDiscoveryCache, trackerStatus *trackerStatusCache, udpProbes *udpProbeCache, udpProbeRequests *udpProbeRequestCache, excluded map[string]bool, peerLoad *peerLoadState, peerUsage *peerUsageState) ([]scheduler.PeerCandidate, error) {
	now := time.Now()
	var candidates []scheduler.PeerCandidate
	keepAliveRecentUDPSuccesses(logger, options.contentID, options.trackerURL, options.selfUDPListenAddr, now)
	if discoveryCache != nil {
		if cached, ok := discoveryCache.Load(options.contentID, now); ok {
			return filterPeerCandidates(logger, options.contentID, cached, peerHealth, excluded, now, peerLoad, peerUsage)
		}
	}

	peerAddrs := collectPeerAddrs(options.explicitPeer, options.explicitPeers)
	if options.lanEnabled {
		discoveredAddrs, err := discoverLANPeers(logger, options.contentID, options.lanAddr, options.selfListenAddr)
		if err != nil {
			return nil, err
		}
		peerAddrs = appendUnique(peerAddrs, discoveredAddrs...)
	}
	if options.trackerURL != "" {
		discoveredAddrs, err := discoverTrackerPeers(logger, options.contentID, options.trackerURL, options.selfListenAddr)
		if err != nil {
			return nil, err
		}
		peerAddrs = appendUnique(peerAddrs, discoveredAddrs...)
	}
	udpPeerAddrs := collectPeerAddrs(options.explicitUDPPeer, options.explicitUDPPeers)
	udpPreferences := make(map[string]udpPeerPreference)
	if options.trackerURL != "" {
		discoveredUDPAddrs, discoveredPreferences, err := discoverTrackerUDPPeers(logger, options.contentID, options.trackerURL, options.selfListenAddr, options.selfUDPListenAddr, udpProbeRequests, trackerStatus)
		if err != nil {
			return nil, err
		}
		udpPeerAddrs = appendUnique(udpPeerAddrs, discoveredUDPAddrs...)
		for addr, preference := range discoveredPreferences {
			udpPreferences[addr] = preference
		}
	}

	freshCandidates, err := collectPeerCandidates(logger, options.contentID, peerAddrs, udpPeerAddrs, options.selfUDPListenAddr, udpPreferences, peerHealth, udpProbes)
	if err != nil {
		return nil, err
	}
	if discoveryCache != nil {
		discoveryCache.Store(options.contentID, freshCandidates, now)
	}
	candidates = freshCandidates
	return filterPeerCandidates(logger, options.contentID, candidates, peerHealth, excluded, now, peerLoad, peerUsage)
}

func keepAliveRecentUDPSuccesses(logger *logging.Logger, contentID string, trackerURL string, selfUDPListenAddr string, now time.Time) {
	if strings.TrimSpace(selfUDPListenAddr) == "" {
		return
	}
	for _, remoteAddr := range p2pnet.RecentUDPSuccessAddrs(contentID, 30*time.Second, now) {
		if !p2pnet.ShouldKeepAliveRecentUDPSuccess(selfUDPListenAddr, contentID, remoteAddr, 6*time.Second, now) {
			continue
		}
		go func(remote string) {
			if err := p2pnet.NewUDPClient(remote, 1200*time.Millisecond).WithLocalAddr(selfUDPListenAddr).Probe(); err != nil {
				reportTrackerUDPKeepalive(logger, trackerURL, "udp://"+remote, contentID, false, udpDiscoveryErrorKind(err))
				logger.Info("udp_success_keepalive_failed",
					"contentId", contentID,
					"remote", remote,
					"error", err.Error(),
				)
				return
			}
			reportTrackerUDPKeepalive(logger, trackerURL, "udp://"+remote, contentID, true, "")
			logger.Info("udp_success_keepalive_sent",
				"contentId", contentID,
				"remote", remote,
			)
		}(remoteAddr)
	}
}

func reportTrackerUDPKeepalive(logger *logging.Logger, trackerURL string, targetPeerID string, contentID string, success bool, errorKind string) {
	if strings.TrimSpace(trackerURL) == "" || strings.TrimSpace(targetPeerID) == "" || strings.TrimSpace(contentID) == "" {
		return
	}
	client := tracker.NewClient(trackerURL)
	if err := client.ReportUDPKeepaliveResult(context.Background(), targetPeerID, contentID, success, errorKind); err != nil {
		logger.Error("tracker_udp_keepalive_report_failed",
			"contentId", contentID,
			"targetPeerId", targetPeerID,
			"tracker", trackerURL,
			"error", err.Error(),
		)
	}
}

func collectPeerCandidates(logger *logging.Logger, contentID string, peerAddrs []string, udpPeerAddrs []string, selfUDPListenAddr string, udpPreferences map[string]udpPeerPreference, peerHealth *peerHealthState, udpProbes *udpProbeCache) ([]scheduler.PeerCandidate, error) {
	candidates := make([]scheduler.PeerCandidate, 0, len(peerAddrs)+len(udpPeerAddrs))
	for _, addr := range peerAddrs {
		client := p2pnet.NewClient(addr, 10*time.Second)
		haveRanges, err := client.FetchHave(contentID)
		if err != nil {
			var cooldown time.Duration
			if peerHealth != nil {
				cooldown = peerHealth.MarkFailure(addr, time.Now())
			}
			logger.Error("peer_have_failed",
				"contentId", contentID,
				"peer", addr,
				"cooldownMs", cooldown.Milliseconds(),
				"error", err.Error(),
			)
			continue
		}
		if peerHealth != nil {
			peerHealth.MarkSuccess(addr)
		}
		logger.Info("peer_have_received", "contentId", contentID, "peer", addr, "ranges", haveRanges)
		candidates = append(candidates, scheduler.PeerCandidate{
			PeerID:     addr,
			Addr:       addr,
			Transport:  "tcp",
			IsLAN:      isLANAddr(addr),
			Score:      1,
			HaveRanges: haveRanges,
		})
	}
	for _, addr := range udpPeerAddrs {
		peerID := "udp://" + addr
		burstProfile := strings.TrimSpace(udpPreferences[addr].burstProfile)
		probeClient := p2pnet.NewUDPClient(addr, udpProbeTimeoutForProfile(burstProfile)).WithLocalAddr(selfUDPListenAddr)
		now := time.Now()
		if ok, cached := udpProbes.Load(addr, now); cached {
			if !ok {
				logger.Info("udp_peer_probe_cached_failed", "contentId", contentID, "peer", peerID)
				continue
			}
		} else {
			if err := probeClient.Probe(); err != nil {
				recordUDPBurstOutcome(contentID, peerID, normalizedBurstProfile(burstProfile), "probe", false, time.Now())
				udpProbes.Store(addr, false, now)
				var cooldown time.Duration
				if peerHealth != nil {
					cooldown = peerHealth.MarkFailure(peerID, now)
				}
				logger.Error("udp_peer_probe_failed",
					"contentId", contentID,
					"peer", peerID,
					"errorKind", udpDiscoveryErrorKind(err),
					"cooldownMs", cooldown.Milliseconds(),
					"error", err.Error(),
				)
				continue
			}
			recordUDPBurstOutcome(contentID, peerID, normalizedBurstProfile(burstProfile), "probe", true, time.Now())
			udpProbes.Store(addr, true, now)
			logger.Info("udp_peer_probe_ok", "contentId", contentID, "peer", peerID)
		}
		haveClient := p2pnet.NewUDPClient(addr, udpHaveTimeoutForProfile(burstProfile)).WithLocalAddr(selfUDPListenAddr)
		haveRanges, err := haveClient.FetchHave(contentID)
		if err != nil {
			recordUDPBurstOutcome(contentID, peerID, normalizedBurstProfile(burstProfile), "have", false, time.Now())
			var cooldown time.Duration
			errorKind := udpDiscoveryErrorKind(err)
			if peerHealth != nil {
				cooldown = peerHealth.MarkFailureKind(peerID, errorKind, time.Now())
			}
			logger.Error("udp_peer_have_failed",
				"contentId", contentID,
				"peer", peerID,
				"errorKind", errorKind,
				"cooldownMs", cooldown.Milliseconds(),
				"error", err.Error(),
			)
			continue
		}
		recordUDPBurstOutcome(contentID, peerID, normalizedBurstProfile(burstProfile), "have", true, time.Now())
		if peerHealth != nil {
			peerHealth.MarkSuccess(peerID)
		}
		logger.Info("udp_peer_have_received", "contentId", contentID, "peer", peerID, "ranges", haveRanges)
		candidates = append(candidates, scheduler.PeerCandidate{
			PeerID:          peerID,
			Addr:            addr,
			Transport:       "udp",
			IsLAN:           isLANAddr(addr),
			Score:           udpCandidateScore(addr, udpPreferences, time.Now()),
			BurstProfile:    burstProfile,
			UDPDecisionRisk: strings.TrimSpace(udpPreferences[addr].decisionRisk),
			HaveRanges:      haveRanges,
		})
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no reachable peers for content %s", contentID)
	}
	return candidates, nil
}

func udpDiscoveryErrorKind(err error) string {
	if err == nil {
		return ""
	}
	if p2pnet.IsUDPTimeout(err) {
		return "udp_timeout"
	}
	return "generic"
}

func udpCandidateScore(addr string, udpPreferences map[string]udpPeerPreference, now time.Time) float64 {
	score := 1.2
	preference, ok := udpPreferences[addr]
	if !ok {
		return score
	}
	if !preference.observedAt.IsZero() {
		age := now.Sub(preference.observedAt)
		switch {
		case age <= 5*time.Second:
			score = 1.8
		case age <= 15*time.Second:
			score = 1.6
		case age <= 30*time.Second:
			score = 1.4
		}
	}
	score += preference.trackerBias
	return score
}

func udpProbeTimeoutForProfile(profile string) time.Duration {
	switch strings.TrimSpace(profile) {
	case "warm":
		return 2200 * time.Millisecond
	case "aggressive":
		return 3800 * time.Millisecond
	default:
		return 3 * time.Second
	}
}

func udpHaveTimeoutForProfile(profile string) time.Duration {
	switch strings.TrimSpace(profile) {
	case "warm":
		return 2600 * time.Millisecond
	case "aggressive":
		return 4300 * time.Millisecond
	default:
		return 3400 * time.Millisecond
	}
}

func filterPeerCandidates(logger *logging.Logger, contentID string, candidates []scheduler.PeerCandidate, peerHealth *peerHealthState, excluded map[string]bool, now time.Time, peerLoad *peerLoadState, peerUsage *peerUsageState) ([]scheduler.PeerCandidate, error) {
	filtered := make([]scheduler.PeerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if excluded[candidate.PeerID] {
			continue
		}
		if peerHealth != nil && !peerHealth.IsAvailable(candidate.PeerID, now) {
			logger.Info("peer_cooling_down",
				"contentId", contentID,
				"peer", candidate.PeerID,
				"remainingMs", peerHealth.RemainingCooldown(candidate.PeerID, now).Milliseconds(),
			)
			continue
		}
		if peerLoad != nil {
			candidate.PendingCount = peerLoad.PendingCount(candidate.PeerID)
		}
		if peerUsage != nil {
			candidate.Score *= 1.0 / float64(1+peerUsage.AssignmentCount(candidate.PeerID))
		}
		filtered = append(filtered, candidate)
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("no reachable peers for content %s", contentID)
	}
	return filtered, nil
}

func trackerUDPProbeBiases(client *tracker.Client, trackerURL string, contentID string, statusCache *trackerStatusCache) map[string]float64 {
	now := time.Now()
	status, ok := loadTrackerStatus(client, trackerURL, statusCache, now)
	if !ok {
		return map[string]float64{}
	}
	return buildTrackerUDPPeerBiases(status, contentID, now)
}

func buildTrackerUDPPeerBiases(status tracker.StatusResponse, contentID string, now time.Time) map[string]float64 {
	biases := make(map[string]float64, len(status.UDPProbeResults))
	for _, result := range status.UDPProbeResults {
		biases[result.TargetPeerID] = trackerUDPProbeBias(result, now)
	}
	probeResults := make(map[string]tracker.UDPProbeResultStatus, len(status.UDPProbeResults))
	for _, result := range status.UDPProbeResults {
		probeResults[result.TargetPeerID] = result
	}
	transferPaths := make(map[string]tracker.PeerTransferPathStatus, len(status.PeerTransferPaths))
	for _, path := range status.PeerTransferPaths {
		if strings.TrimSpace(path.ContentID) != "" && path.ContentID != contentID {
			continue
		}
		transferPaths[path.TargetPeerID] = path
	}
	keepaliveResults := make(map[string]tracker.UDPKeepaliveStatus, len(status.UDPKeepaliveResults))
	for _, item := range status.UDPKeepaliveResults {
		if strings.TrimSpace(item.ContentID) != "" && item.ContentID != contentID {
			continue
		}
		keepaliveResults[item.TargetPeerID] = item
	}
	udpDecisions := make(map[string]tracker.UDPDecisionStatus, len(status.UDPDecisions))
	for _, item := range status.UDPDecisions {
		if strings.TrimSpace(item.ContentID) != "" && item.ContentID != contentID {
			continue
		}
		udpDecisions[item.TargetPeerID] = item
	}
	for _, swarm := range status.Swarms {
		if swarm.ContentID != contentID {
			continue
		}
		for _, peer := range swarm.Peers {
			transferBias := trackerTransferPathBias(peer, probeResults[peer.PeerID], transferPaths[peer.PeerID], now)
			keepaliveBias := trackerPeerUDPKeepaliveBias(peer, keepaliveResults, now)
			keepaliveResult := peerKeepaliveResult(peer, keepaliveResults)
			fallbackBias := trackerUDPFallbackBias(transferPaths[peer.PeerID], keepaliveResult, now)
			_, _, _, burstProfile := trackerPeerUDPState(status, contentID, peer)
			burstBias := trackerBurstProfileBias(burstProfile, now)
			decisionBias := trackerUDPDecisionBias(peer, probeResults[peer.PeerID], transferPaths[peer.PeerID], udpDecisions[peer.PeerID], now)
			biases[peer.PeerID] += transferBias + keepaliveBias + fallbackBias + burstBias + decisionBias
		}
	}
	return biases
}

func trackerUDPDecisionBias(peer tracker.PeerRecord, probeResult tracker.UDPProbeResultStatus, transferPath tracker.PeerTransferPathStatus, decision tracker.UDPDecisionStatus, now time.Time) float64 {
	if strings.TrimSpace(decision.TargetPeerID) == "" || decision.LastReportedAt == 0 {
		return 0
	}
	if now.Sub(time.Unix(decision.LastReportedAt, 0)) > 45*time.Second {
		return 0
	}
	switch trackerUDPDecisionRiskKind(peer, probeResult, transferPath, decision) {
	case "low":
		return -0.22
	case "warn":
		return -0.10
	case "recovering":
		return 0.08
	case "stable":
		return 0.14
	default:
		return 0
	}
}

func trackerUDPDecisionRiskKind(peer tracker.PeerRecord, probeResult tracker.UDPProbeResultStatus, transferPath tracker.PeerTransferPathStatus, decision tracker.UDPDecisionStatus) string {
	if strings.TrimSpace(decision.BurstProfile) == "" {
		return ""
	}
	advice := discoveryTrackerPeerRouteAdvice(peer, probeResult)
	drift := discoveryTrackerRouteDrift(advice, transferPath.LastPath)
	profile := strings.TrimSpace(decision.BurstProfile)
	stage := strings.TrimSpace(decision.LastStage)

	if drift == "udp_miss" && profile == "aggressive" && stage == "probe" && decision.ReportCount >= 2 {
		return "low"
	}
	if drift == "udp_miss" && stage == "have" {
		return "warn"
	}
	if drift != "udp_miss" && profile == "warm" && stage == "piece" {
		return "stable"
	}
	if (drift == "-" || drift == "aligned" || drift == "udp_recovered") && stage == "piece" {
		return "recovering"
	}
	return ""
}

func trackerUDPProbeBias(result tracker.UDPProbeResultStatus, now time.Time) float64 {
	if result.LastSuccessAt > 0 {
		age := now.Sub(time.Unix(result.LastSuccessAt, 0))
		switch {
		case age <= 10*time.Second:
			return 0.25
		case age <= 30*time.Second:
			return 0.15
		}
	}
	if result.LastFailureAt > 0 {
		age := now.Sub(time.Unix(result.LastFailureAt, 0))
		penalty := trackerUDPFailurePenalty(result.LastErrorKind)
		switch {
		case age <= 10*time.Second:
			return penalty
		case age <= 30*time.Second:
			return penalty * 0.6
		}
	}
	return 0
}

func trackerUDPFailurePenalty(errorKind string) float64 {
	switch strings.TrimSpace(errorKind) {
	case peerFailureKindUDPTimeout:
		return -0.18
	case "", peerFailureKindGeneric:
		return -0.30
	default:
		return -0.30
	}
}

func trackerTransferPathBias(peer tracker.PeerRecord, probeResult tracker.UDPProbeResultStatus, transferPath tracker.PeerTransferPathStatus, now time.Time) float64 {
	if strings.TrimSpace(transferPath.LastPath) == "" || transferPath.LastAt == 0 {
		return 0
	}
	advice := discoveryTrackerPeerRouteAdvice(peer, probeResult)
	drift := discoveryTrackerRouteDrift(advice, transferPath.LastPath)
	age := now.Sub(time.Unix(transferPath.LastAt, 0))
	switch drift {
	case "udp_miss":
		switch {
		case age <= 10*time.Second:
			return -0.28
		case age <= 30*time.Second:
			return -0.18
		case age <= time.Minute:
			return -0.10
		default:
			return 0
		}
	case "udp_recovered":
		switch {
		case age <= 10*time.Second:
			return 0.12
		case age <= 30*time.Second:
			return 0.08
		case age <= time.Minute:
			return 0.04
		default:
			return 0
		}
	default:
		return 0
	}
}

func trackerUDPKeepaliveBias(result tracker.UDPKeepaliveStatus, now time.Time) float64 {
	if result.LastSuccessAt > 0 {
		age := now.Sub(time.Unix(result.LastSuccessAt, 0))
		switch {
		case age <= 10*time.Second:
			return 0.10
		case age <= 30*time.Second:
			return 0.06
		}
	}
	if result.LastFailureAt > 0 {
		age := now.Sub(time.Unix(result.LastFailureAt, 0))
		penalty := trackerUDPKeepaliveFailurePenalty(result.LastErrorKind)
		switch {
		case age <= 10*time.Second:
			return penalty
		case age <= 30*time.Second:
			return penalty * 0.6
		}
	}
	return 0
}

func trackerUDPKeepaliveFailurePenalty(errorKind string) float64 {
	switch strings.TrimSpace(errorKind) {
	case peerFailureKindUDPTimeout:
		return -0.12
	case "", peerFailureKindGeneric:
		return -0.18
	default:
		return -0.18
	}
}

func trackerPeerUDPKeepaliveBias(peer tracker.PeerRecord, keepaliveResults map[string]tracker.UDPKeepaliveStatus, now time.Time) float64 {
	result := peerKeepaliveResult(peer, keepaliveResults)
	if result.TargetPeerID == "" {
		return 0
	}
	return trackerUDPKeepaliveBias(result, now)
}

func peerKeepaliveResult(peer tracker.PeerRecord, keepaliveResults map[string]tracker.UDPKeepaliveStatus) tracker.UDPKeepaliveStatus {
	best := tracker.UDPKeepaliveStatus{}
	for _, key := range peerKeepaliveKeys(peer) {
		result, ok := keepaliveResults[key]
		if !ok {
			continue
		}
		if trackerUDPKeepaliveRecency(result) > trackerUDPKeepaliveRecency(best) {
			best = result
		}
	}
	return best
}

func peerKeepaliveKeys(peer tracker.PeerRecord) []string {
	keys := make([]string, 0, len(peer.UDPAddrs)+1)
	if peer.ObservedUDPAddr != "" {
		keys = append(keys, "udp://"+peer.ObservedUDPAddr)
	}
	for _, addr := range peer.UDPAddrs {
		if strings.TrimSpace(addr) == "" {
			continue
		}
		key := "udp://" + addr
		duplicate := false
		for _, existing := range keys {
			if existing == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			keys = append(keys, key)
		}
	}
	return keys
}

func trackerUDPKeepaliveRecency(result tracker.UDPKeepaliveStatus) int64 {
	if result.LastSuccessAt > result.LastFailureAt {
		return result.LastSuccessAt
	}
	return result.LastFailureAt
}

func trackerUDPFallbackBias(transferPath tracker.PeerTransferPathStatus, keepaliveResult tracker.UDPKeepaliveStatus, now time.Time) float64 {
	if strings.TrimSpace(transferPath.LastPath) != "tcp" || transferPath.LastAt == 0 {
		return 0
	}
	if keepaliveResult.LastFailureAt == 0 || keepaliveResult.LastFailureAt < keepaliveResult.LastSuccessAt {
		return 0
	}
	transferAge := now.Sub(time.Unix(transferPath.LastAt, 0))
	keepaliveAge := now.Sub(time.Unix(keepaliveResult.LastFailureAt, 0))
	switch {
	case transferAge <= 12*time.Second && keepaliveAge <= 12*time.Second:
		return -0.16
	case transferAge <= 30*time.Second && keepaliveAge <= 30*time.Second:
		return -0.08
	default:
		return 0
	}
}

func discoveryTrackerPeerRouteAdvice(peer tracker.PeerRecord, result tracker.UDPProbeResultStatus) string {
	hasUDPPath := len(peer.UDPAddrs) > 0 || strings.TrimSpace(peer.ObservedUDPAddr) != ""
	if !hasUDPPath {
		return "tcp_only"
	}
	if result.TargetPeerID == "" {
		if strings.TrimSpace(peer.ObservedUDPAddr) != "" {
			return "prefer_udp"
		}
		return "try_udp"
	}
	if result.LastSuccessAt > result.LastFailureAt {
		return "prefer_udp"
	}
	if result.LastFailureAt > 0 {
		if strings.TrimSpace(result.LastErrorKind) == peerFailureKindUDPTimeout {
			return "udp_fallback"
		}
		return "prefer_tcp"
	}
	if strings.TrimSpace(peer.ObservedUDPAddr) != "" {
		return "prefer_udp"
	}
	return "try_udp"
}

func discoveryTrackerRouteDrift(advice string, actual string) string {
	recommended := ""
	switch advice {
	case "prefer_udp", "try_udp", "udp_fallback":
		recommended = "udp"
	case "prefer_tcp", "tcp_only":
		recommended = "tcp"
	}
	actual = strings.TrimSpace(strings.ToLower(actual))
	if recommended == "" || actual == "" {
		return "-"
	}
	if recommended == "udp" && actual == "tcp" {
		return "udp_miss"
	}
	if recommended == "tcp" && actual == "udp" {
		return "udp_recovered"
	}
	return "aligned"
}

func maxFloat64(left float64, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func discoverLANPeers(logger *logging.Logger, contentID string, lanAddr string, selfListenAddr string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()

	listenAddr := lanAddr
	if host, port, ok := strings.Cut(lanAddr, ":"); ok && host != "" && port != "" {
		listenAddr = ":" + port
	}

	peers, err := p2pnet.DiscoverLAN(ctx, listenAddr, contentID, selfListenAddr)
	if err != nil {
		return nil, fmt.Errorf("discover lan peers: %w", err)
	}

	addrs := make([]string, 0, len(peers))
	for _, peer := range peers {
		logger.Info("lan_peer_discovered",
			"contentId", contentID,
			"peerId", peer.PeerID,
			"listen", peer.ListenAddr,
			"ranges", peer.HaveRanges,
		)
		addrs = append(addrs, peer.ListenAddr)
	}
	return addrs, nil
}

func isLANAddr(addr string) bool {
	addr = strings.TrimPrefix(addr, "udp://")
	addr = strings.TrimPrefix(addr, "tcp://")
	return strings.HasPrefix(addr, "127.") ||
		strings.HasPrefix(addr, "10.") ||
		strings.HasPrefix(addr, "192.168.") ||
		strings.HasPrefix(addr, "172.16.") ||
		strings.HasPrefix(addr, "172.17.") ||
		strings.HasPrefix(addr, "172.18.") ||
		strings.HasPrefix(addr, "172.19.") ||
		strings.HasPrefix(addr, "172.2")
}

func transferPathForPeer(addr string) string {
	if isLANAddr(addr) {
		return "lan"
	}
	return "direct"
}
