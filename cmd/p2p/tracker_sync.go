package main

import (
	"context"
	"fmt"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/logging"
	"generic-p2p/internal/tracker"
)

func syncTrackerProgress(logger *logging.Logger, discovery peerDiscoveryOptions, store *core.PieceStore) error {
	if discovery.trackerURL == "" || discovery.selfListenAddr == "" {
		return nil
	}
	return syncTrackerPeer(logger, discovery.trackerURL, discovery.selfListenAddr, discovery.contentID, store.CompletedRanges())
}

func startTrackerSyncLoop(logger *logging.Logger, trackerURL string, peerID string, contentID string, interval time.Duration, haveRanges func() []core.HaveRange) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	syncOnce := func() {
		if err := syncTrackerPeer(logger, trackerURL, peerID, contentID, haveRanges()); err != nil {
			logger.Error("tracker_sync_failed", "tracker", trackerURL, "peer", peerID, "contentId", contentID, "error", err.Error())
		}
	}

	syncOnce()
	for range ticker.C {
		syncOnce()
	}
}

func syncTrackerPeer(logger *logging.Logger, trackerURL string, peerID string, contentID string, haveRanges []core.HaveRange) error {
	client := tracker.NewClient(trackerURL)
	if err := client.RegisterPeer(context.Background(), peerID, []string{peerID}); err != nil {
		return fmt.Errorf("register peer: %w", err)
	}
	if err := client.JoinSwarm(context.Background(), peerID, contentID, haveRanges); err != nil {
		return fmt.Errorf("join swarm: %w", err)
	}
	return nil
}
