package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"generic-p2p/internal/logging"
	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/scheduler"
	"generic-p2p/internal/tracker"
)

type udpPeerPreference struct {
	observedAt  time.Time
	trackerBias float64
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

	probeBiases := trackerUDPProbeBiases(client, trackerURL, contentID, trackerStatus)
	addrs := make([]string, 0, len(peers))
	preferences := make(map[string]udpPeerPreference)
	for _, peer := range peers {
		now := time.Now()
		probeRequestKey := contentID + "|" + selfPeerID + "|" + selfUDPListenAddr + "|" + peer.PeerID
		if selfPeerID != "" && selfUDPListenAddr != "" && peer.PeerID != "" && peer.PeerID != selfPeerID && udpProbeRequests.ShouldRequest(probeRequestKey, now) {
			if err := client.RequestUDPProbe(context.Background(), contentID, selfPeerID, selfUDPListenAddr, peer.PeerID); err != nil {
				logger.Error("tracker_udp_probe_request_failed",
					"contentId", contentID,
					"peerId", peer.PeerID,
					"error", err.Error(),
				)
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
			preferences[observedAddr] = pref
			maybeKeepAliveUDPPath(logger, contentID, peer.PeerID, selfUDPListenAddr, observedAddr, now)
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
			preferences[peer.ObservedUDPAddr] = pref
			maybeKeepAliveUDPPath(logger, contentID, peer.PeerID, selfUDPListenAddr, peer.ObservedUDPAddr, now)
		}
	}
	return addrs, preferences, nil
}

func maybeKeepAliveUDPPath(logger *logging.Logger, contentID string, peerID string, selfUDPListenAddr string, remoteAddr string, now time.Time) {
	if !p2pnet.ShouldKeepAliveObservedUDPPeer(selfUDPListenAddr, contentID, peerID, remoteAddr, 8*time.Second, now) {
		return
	}
	go func() {
		if err := p2pnet.NewUDPClient(remoteAddr, 1500*time.Millisecond).WithLocalAddr(selfUDPListenAddr).ProbeForPeer(contentID, peerID); err != nil {
			logger.Info("udp_keepalive_failed",
				"contentId", contentID,
				"peerId", peerID,
				"remote", remoteAddr,
				"error", err.Error(),
			)
			return
		}
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
		client := p2pnet.NewUDPClient(addr, 3*time.Second).WithLocalAddr(selfUDPListenAddr)
		now := time.Now()
		if ok, cached := udpProbes.Load(addr, now); cached {
			if !ok {
				logger.Info("udp_peer_probe_cached_failed", "contentId", contentID, "peer", peerID)
				continue
			}
		} else {
			if err := client.Probe(); err != nil {
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
			udpProbes.Store(addr, true, now)
			logger.Info("udp_peer_probe_ok", "contentId", contentID, "peer", peerID)
		}
		haveRanges, err := client.FetchHave(contentID)
		if err != nil {
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
		if peerHealth != nil {
			peerHealth.MarkSuccess(peerID)
		}
		logger.Info("udp_peer_have_received", "contentId", contentID, "peer", peerID, "ranges", haveRanges)
		candidates = append(candidates, scheduler.PeerCandidate{
			PeerID:     peerID,
			Addr:       addr,
			Transport:  "udp",
			IsLAN:      isLANAddr(addr),
			Score:      udpCandidateScore(addr, udpPreferences, time.Now()),
			HaveRanges: haveRanges,
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
	if statusCache != nil {
		if cached, ok := statusCache.Load(trackerURL, now); ok {
			return buildTrackerUDPPeerBiases(cached, contentID, now)
		}
	}

	status, err := client.GetStatus(context.Background())
	if err != nil {
		return map[string]float64{}
	}
	if statusCache != nil {
		statusCache.Store(trackerURL, status, now)
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
	for _, swarm := range status.Swarms {
		if swarm.ContentID != contentID {
			continue
		}
		for _, peer := range swarm.Peers {
			biases[peer.PeerID] += trackerTransferPathBias(peer, probeResults[peer.PeerID], transferPaths[peer.PeerID], now)
		}
	}
	return biases
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
