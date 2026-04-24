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
	}
	return addrs, nil
}

func discoverTrackerUDPPeers(logger *logging.Logger, contentID string, trackerURL string, selfUDPListenAddr string) ([]string, error) {
	client := tracker.NewClient(trackerURL)
	peers, err := client.GetPeers(context.Background(), contentID)
	if err != nil {
		return nil, fmt.Errorf("discover tracker udp peers: %w", err)
	}

	addrs := make([]string, 0, len(peers))
	for _, peer := range peers {
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
		}
	}
	return addrs, nil
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

func collectDynamicPeerCandidates(logger *logging.Logger, options peerDiscoveryOptions, peerHealth *peerHealthState, discoveryCache *peerDiscoveryCache, excluded map[string]bool, peerLoad *peerLoadState, peerUsage *peerUsageState) ([]scheduler.PeerCandidate, error) {
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
	if options.trackerURL != "" {
		discoveredUDPAddrs, err := discoverTrackerUDPPeers(logger, options.contentID, options.trackerURL, options.selfUDPListenAddr)
		if err != nil {
			return nil, err
		}
		udpPeerAddrs = appendUnique(udpPeerAddrs, discoveredUDPAddrs...)
	}

	freshCandidates, err := collectPeerCandidates(logger, options.contentID, peerAddrs, udpPeerAddrs, peerHealth)
	if err != nil {
		return nil, err
	}
	if discoveryCache != nil {
		discoveryCache.Store(options.contentID, freshCandidates, now)
	}
	candidates = freshCandidates
	return filterPeerCandidates(logger, options.contentID, candidates, peerHealth, excluded, now, peerLoad, peerUsage)
}

func collectPeerCandidates(logger *logging.Logger, contentID string, peerAddrs []string, udpPeerAddrs []string, peerHealth *peerHealthState) ([]scheduler.PeerCandidate, error) {
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
		client := p2pnet.NewUDPClient(addr, 10*time.Second)
		haveRanges, err := client.FetchHave(contentID)
		if err != nil {
			var cooldown time.Duration
			if peerHealth != nil {
				cooldown = peerHealth.MarkFailure(peerID, time.Now())
			}
			logger.Error("udp_peer_have_failed",
				"contentId", contentID,
				"peer", peerID,
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
			Score:      1.2,
			HaveRanges: haveRanges,
		})
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no reachable peers for content %s", contentID)
	}
	return candidates, nil
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
			candidate.Score = 1.0 / float64(1+peerUsage.AssignmentCount(candidate.PeerID))
		}
		filtered = append(filtered, candidate)
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("no reachable peers for content %s", contentID)
	}
	return filtered, nil
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
