package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/logging"
	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/tracker"
)

func syncTrackerProgress(logger *logging.Logger, discovery peerDiscoveryOptions, store *core.PieceStore) error {
	if discovery.trackerURL == "" || discovery.selfListenAddr == "" {
		return nil
	}
	return syncTrackerPeer(logger, discovery.trackerURL, discovery.selfListenAddr, discovery.selfUDPListenAddr, discovery.stunServer, &stunObservedAddrCache{}, store.RuntimeStats(), discovery.contentID, store.CompletedRanges())
}

type stunObservedAddrCache struct {
	mu        sync.Mutex
	server    string
	localAddr string
	value     string
	expiresAt time.Time
}

func (c *stunObservedAddrCache) Resolve(logger *logging.Logger, server string, localAddr string, now time.Time) string {
	server = strings.TrimSpace(server)
	localAddr = strings.TrimSpace(localAddr)
	if server == "" || localAddr == "" {
		return ""
	}
	c.mu.Lock()
	if c.server == server && c.localAddr == localAddr && now.Before(c.expiresAt) {
		value := c.value
		c.mu.Unlock()
		return value
	}
	c.mu.Unlock()

	addr, err := p2pnet.DiscoverSTUNMappedAddr(server, localAddr, 3*time.Second)
	ttl := 20 * time.Second
	if err != nil {
		ttl = 5 * time.Second
		logger.Error("stun_discovery_failed", "server", server, "localAddr", localAddr, "error", err.Error())
	}

	c.mu.Lock()
	c.server = server
	c.localAddr = localAddr
	c.value = strings.TrimSpace(addr)
	c.expiresAt = now.Add(ttl)
	c.mu.Unlock()

	if strings.TrimSpace(addr) != "" {
		logger.Info("stun_udp_addr_discovered", "server", server, "localAddr", localAddr, "observedUdpAddr", addr)
	}
	return strings.TrimSpace(addr)
}

func startTrackerSyncLoop(logger *logging.Logger, trackerURL string, peerID string, udpAddr string, stunServer string, runtime *core.RuntimeStats, contentID string, interval time.Duration, haveRanges func() []core.HaveRange) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	stunCache := &stunObservedAddrCache{}

	syncOnce := func() {
		if err := syncTrackerPeer(logger, trackerURL, peerID, udpAddr, stunServer, stunCache, runtime, contentID, haveRanges()); err != nil {
			logger.Error("tracker_sync_failed", "tracker", trackerURL, "peer", peerID, "contentId", contentID, "error", err.Error())
		}
	}

	syncOnce()
	for range ticker.C {
		syncOnce()
	}
}

func syncTrackerPeer(logger *logging.Logger, trackerURL string, peerID string, udpAddr string, stunServer string, stunCache *stunObservedAddrCache, runtime *core.RuntimeStats, contentID string, haveRanges []core.HaveRange) error {
	client := tracker.NewClient(trackerURL)
	now := time.Now()
	udpAddrs := []string(nil)
	observedUDPAddr := ""
	if udpAddr != "" {
		udpAddrs = []string{udpAddr}
		if stunCache != nil {
			observedUDPAddr = stunCache.Resolve(logger, stunServer, udpAddr, now)
		}
		if runtime != nil && strings.TrimSpace(observedUDPAddr) != "" {
			_ = runtime.SetUDPObservation(core.UDPObservationStatus{
				ObservedUDPAddr: observedUDPAddr,
				Source:          "stun",
				Server:          strings.TrimSpace(stunServer),
				ObservedAt:      now.Format(time.RFC3339),
			})
		}
	}
	if err := client.RegisterPeerWithUDPObserved(context.Background(), peerID, []string{peerID}, udpAddrs, observedUDPAddr, "stun"); err != nil {
		return fmt.Errorf("register peer: %w", err)
	}
	if err := client.JoinSwarm(context.Background(), peerID, contentID, haveRanges); err != nil {
		return fmt.Errorf("join swarm: %w", err)
	}
	if udpAddr != "" {
		if err := pollTrackerUDPProbeRequests(logger, client, peerID, udpAddr); err != nil {
			return fmt.Errorf("poll udp probes: %w", err)
		}
		maintainTrackerUDPSessions(logger, client, peerID, udpAddr, contentID, now)
		if err := reportTrackerUDPSessions(client, contentID, now); err != nil {
			return fmt.Errorf("report udp sessions: %w", err)
		}
	}
	return nil
}

func pollTrackerUDPProbeRequests(logger *logging.Logger, client *tracker.Client, peerID string, udpAddr string) error {
	requests, err := client.PollUDPProbeRequests(context.Background(), peerID)
	if err != nil {
		return err
	}

	for _, request := range requests {
		targets := responderSideBurstPunchTargets(request)
		if len(targets) == 0 {
			continue
		}
		phases := adaptiveResponderBurstPhases(request, time.Now())
		profile := udpBurstProfileName(phases)
		succeeded := false
		var lastErr error
		for _, targetAddr := range targets {
			noteUDPSessionAddr(request.RequesterPeerID, targetAddr, "tracker_probe_request", request.ContentID, time.Now())
			if err := p2pnet.NewUDPClient(targetAddr, 2*time.Second).WithLocalAddr(udpAddr).ProbeMultiBurstForPeer(request.ContentID, peerID, phases); err != nil {
				lastErr = err
				noteUDPSessionStageFailure(request.RequesterPeerID, targetAddr, "probe", trackerProbeErrorKind(err), time.Now())
				logger.Error("tracker_udp_probe_response_failed",
					"contentId", request.ContentID,
					"requesterPeerId", request.RequesterPeerID,
					"profile", profile,
					"target", targetAddr,
					"error", err.Error(),
				)
				continue
			}
			succeeded = true
			noteUDPSessionStageSuccess(request.RequesterPeerID, targetAddr, "probe", request.ContentID, time.Now())
			logger.Info("tracker_udp_probe_response_sent",
				"contentId", request.ContentID,
				"requesterPeerId", request.RequesterPeerID,
				"profile", profile,
				"target", targetAddr,
			)
			break
		}
		if !succeeded {
			recordUDPBurstOutcome(request.ContentID, request.RequesterPeerID, profile, "probe", false, time.Now())
			if reportErr := client.ReportUDPProbeResult(context.Background(), peerID, request.RequesterPeerID, request.ContentID, false, trackerProbeErrorKind(lastErr)); reportErr != nil {
				logger.Error("tracker_udp_probe_report_failed",
					"contentId", request.ContentID,
					"requesterPeerId", request.RequesterPeerID,
					"targetPeerId", peerID,
					"error", reportErr.Error(),
				)
			}
			continue
		}
		recordUDPBurstOutcome(request.ContentID, request.RequesterPeerID, profile, "probe", true, time.Now())
		if reportErr := client.ReportUDPProbeResult(context.Background(), peerID, request.RequesterPeerID, request.ContentID, true, ""); reportErr != nil {
			logger.Error("tracker_udp_probe_report_failed",
				"contentId", request.ContentID,
				"requesterPeerId", request.RequesterPeerID,
				"targetPeerId", peerID,
				"error", reportErr.Error(),
			)
		}
	}
	return nil
}

func maintainTrackerUDPSessions(logger *logging.Logger, client *tracker.Client, selfPeerID string, selfUDPListenAddr string, contentID string, now time.Time) {
	if strings.TrimSpace(selfUDPListenAddr) == "" {
		return
	}
	for _, session := range currentUDPSessionSnapshots(contentID, now) {
		if session.PeerID == "" || session.PeerID == selfPeerID {
			continue
		}
		remoteAddr := strings.TrimSpace(session.PrimaryAddr)
		if remoteAddr == "" || !udpSessionShouldSendKeepalive(session.PeerID, remoteAddr, now) {
			continue
		}
		go func(peerID string, remote string) {
			if err := p2pnet.NewUDPClient(remote, 1200*time.Millisecond).WithLocalAddr(selfUDPListenAddr).ProbeForPeer(contentID, peerID); err != nil {
				noteUDPSessionStageFailure(peerID, remote, "keepalive", trackerProbeErrorKind(err), time.Now())
				_ = client.ReportUDPKeepaliveResult(context.Background(), peerID, contentID, false, trackerProbeErrorKind(err))
				logger.Info("tracker_sync_udp_session_keepalive_failed",
					"contentId", contentID,
					"peerId", peerID,
					"remote", remote,
					"error", err.Error(),
				)
				return
			}
			noteUDPSessionStageSuccess(peerID, remote, "keepalive", contentID, time.Now())
			_ = client.ReportUDPKeepaliveResult(context.Background(), peerID, contentID, true, "")
			logger.Info("tracker_sync_udp_session_keepalive_sent",
				"contentId", contentID,
				"peerId", peerID,
				"remote", remote,
			)
		}(session.PeerID, remoteAddr)
	}
}

func reportTrackerUDPSessions(client *tracker.Client, contentID string, now time.Time) error {
	for _, session := range currentUDPSessionSnapshots(contentID, now) {
		if strings.TrimSpace(session.PeerID) == "" || strings.TrimSpace(session.PrimaryAddr) == "" {
			continue
		}
		if err := client.ReportUDPSessionHealth(
			context.Background(),
			session.PeerID,
			contentID,
			session.State,
			session.HealthScore,
			session.LastStage,
			session.LastErrorKind,
			session.RecommendedChunkWindow,
			session.RecommendedRoundTimeout.Milliseconds(),
			session.RecommendedAttemptBudget,
			int(session.KeepaliveInterval/time.Second),
			session.LastActiveAt.Unix(),
			session.LastSuccessAt.Unix(),
			session.LastFailureAt.Unix(),
		); err != nil {
			return err
		}
	}
	return nil
}

func trackerProbeErrorKind(err error) string {
	if err == nil {
		return ""
	}
	if p2pnet.IsUDPTimeout(err) {
		return "udp_timeout"
	}
	return "generic"
}
