package main

import (
	"context"
	"fmt"
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
	return syncTrackerPeer(logger, discovery.trackerURL, discovery.selfListenAddr, discovery.selfUDPListenAddr, discovery.contentID, store.CompletedRanges())
}

func startTrackerSyncLoop(logger *logging.Logger, trackerURL string, peerID string, udpAddr string, contentID string, interval time.Duration, haveRanges func() []core.HaveRange) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	syncOnce := func() {
		if err := syncTrackerPeer(logger, trackerURL, peerID, udpAddr, contentID, haveRanges()); err != nil {
			logger.Error("tracker_sync_failed", "tracker", trackerURL, "peer", peerID, "contentId", contentID, "error", err.Error())
		}
	}

	syncOnce()
	for range ticker.C {
		syncOnce()
	}
}

func syncTrackerPeer(logger *logging.Logger, trackerURL string, peerID string, udpAddr string, contentID string, haveRanges []core.HaveRange) error {
	client := tracker.NewClient(trackerURL)
	udpAddrs := []string(nil)
	if udpAddr != "" {
		udpAddrs = []string{udpAddr}
	}
	if err := client.RegisterPeerWithUDP(context.Background(), peerID, []string{peerID}, udpAddrs); err != nil {
		return fmt.Errorf("register peer: %w", err)
	}
	if err := client.JoinSwarm(context.Background(), peerID, contentID, haveRanges); err != nil {
		return fmt.Errorf("join swarm: %w", err)
	}
	if udpAddr != "" {
		if err := pollTrackerUDPProbeRequests(logger, client, peerID, udpAddr); err != nil {
			return fmt.Errorf("poll udp probes: %w", err)
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
		targetAddr := request.ObservedUDPAddr
		if targetAddr == "" {
			targetAddr = request.RequesterUDPAddr
		}
		if targetAddr == "" {
			continue
		}
		if err := p2pnet.NewUDPClient(targetAddr, 2*time.Second).WithLocalAddr(udpAddr).ProbeMultiBurstForPeer(request.ContentID, peerID, adaptiveResponderBurstPhases(request, time.Now())); err != nil {
			if reportErr := client.ReportUDPProbeResult(context.Background(), peerID, request.RequesterPeerID, request.ContentID, false, trackerProbeErrorKind(err)); reportErr != nil {
				logger.Error("tracker_udp_probe_report_failed",
					"contentId", request.ContentID,
					"requesterPeerId", request.RequesterPeerID,
					"targetPeerId", peerID,
					"error", reportErr.Error(),
				)
			}
			logger.Error("tracker_udp_probe_response_failed",
				"contentId", request.ContentID,
				"requesterPeerId", request.RequesterPeerID,
				"target", targetAddr,
				"error", err.Error(),
			)
			continue
		}
		if reportErr := client.ReportUDPProbeResult(context.Background(), peerID, request.RequesterPeerID, request.ContentID, true, ""); reportErr != nil {
			logger.Error("tracker_udp_probe_report_failed",
				"contentId", request.ContentID,
				"requesterPeerId", request.RequesterPeerID,
				"targetPeerId", peerID,
				"error", reportErr.Error(),
			)
		}
		logger.Info("tracker_udp_probe_response_sent",
			"contentId", request.ContentID,
			"requesterPeerId", request.RequesterPeerID,
			"target", targetAddr,
		)
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
