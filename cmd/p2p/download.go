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

type downloadPieceState struct {
	mu         sync.Mutex
	inProgress map[int]bool
}

func downloadPieces(logger *logging.Logger, manifest *core.ContentManifest, store *core.PieceStore, discovery peerDiscoveryOptions, workers int) error {
	state := &downloadPieceState{
		inProgress: make(map[int]bool),
	}
	peerHealth := newPeerHealthState()
	discoveryCache := newPeerDiscoveryCache(400 * time.Millisecond)
	trackerStatus := newTrackerStatusCache(time.Second)
	udpProbes := newUDPProbeCache(2 * time.Second)
	udpProbeRequests := newUDPProbeRequestCache(3 * time.Second)
	peerLoad := newPeerLoadState()
	peerUsage := newPeerUsageState()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				peerCandidates, err := collectDynamicPeerCandidates(logger, discovery, peerHealth, discoveryCache, trackerStatus, udpProbes, udpProbeRequests, nil, peerLoad, peerUsage)
				if err != nil {
					if allPiecesCompleted(manifest, store) {
						return
					}
					time.Sleep(150 * time.Millisecond)
					continue
				}
				if runtime := store.RuntimeStats(); runtime != nil {
					_ = runtime.SetPeers(len(peerCandidates))
				}

				pieceIndex, ok := reserveNextPiece(manifest, store, state, peerCandidates)
				if !ok {
					if allPiecesCompleted(manifest, store) {
						return
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				err = downloadSinglePiece(logger, manifest, store, discovery, pieceIndex, id, peerHealth, discoveryCache, trackerStatus, udpProbes, udpProbeRequests, peerLoad, peerUsage)

				state.mu.Lock()
				delete(state.inProgress, pieceIndex)
				state.mu.Unlock()

				if err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}(workerID)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-doneCh:
		return nil
	}
}

func downloadSinglePiece(logger *logging.Logger, manifest *core.ContentManifest, store *core.PieceStore, discovery peerDiscoveryOptions, pieceIndex int, workerID int, peerHealth *peerHealthState, discoveryCache *peerDiscoveryCache, trackerStatus *trackerStatusCache, udpProbes *udpProbeCache, udpProbeRequests *udpProbeRequestCache, peerLoad *peerLoadState, peerUsage *peerUsageState) error {
	chooser := scheduler.Scheduler{}
	excluded := make(map[string]bool)

	for attempt := 0; attempt < 5; attempt++ {
		peerCandidates, err := collectDynamicPeerCandidates(logger, discovery, peerHealth, discoveryCache, trackerStatus, udpProbes, udpProbeRequests, excluded, peerLoad, peerUsage)
		if err != nil {
			if attempt == 4 {
				return fmt.Errorf("piece %d candidate refresh failed: %w", pieceIndex, err)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if runtime := store.RuntimeStats(); runtime != nil {
			_ = runtime.SetPeers(len(peerCandidates))
		}

		selected, ok := chooser.ChoosePeer(pieceIndex, peerCandidates)
		if !ok {
			if attempt == 4 {
				return fmt.Errorf("no peer has piece %d", pieceIndex)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		attemptCandidates := pieceAttemptCandidates(pieceIndex, selected, peerCandidates)
		var data []byte
		var usedCandidate scheduler.PeerCandidate
		var lastErr error
		for burstIndex, candidate := range attemptCandidates {
			peerLoad.Acquire(candidate.PeerID)
			if runtime := store.RuntimeStats(); runtime != nil {
				_ = runtime.StartDownload(pieceIndex, candidate.PeerID, workerID, time.Now())
			}
			peerUsage.RecordAssignment(candidate.PeerID)
			data, lastErr = fetchPieceFromCandidate(candidate, manifest.ContentID, pieceIndex)
			peerLoad.Release(candidate.PeerID)
			if runtime := store.RuntimeStats(); runtime != nil {
				_ = runtime.FinishDownload(pieceIndex)
			}
			if lastErr != nil {
				errorKind := transferErrorKind(candidate, lastErr)
				cooldown := peerHealth.MarkFailureKind(candidate.PeerID, errorKind, time.Now())
				logger.Error("piece_download_failed",
					"contentId", manifest.ContentID,
					"pieceIndex", pieceIndex,
					"peer", candidate.PeerID,
					"attempt", attempt+1,
					"burstTry", burstIndex+1,
					"burstPeers", len(attemptCandidates),
					"errorKind", errorKind,
					"cooldownMs", cooldown.Milliseconds(),
					"error", lastErr.Error(),
				)
				excluded[candidate.PeerID] = true
				if burstIndex+1 < len(attemptCandidates) {
					time.Sleep(60 * time.Millisecond)
				}
				continue
			}
			usedCandidate = candidate
			peerHealth.MarkSuccess(candidate.PeerID)
			break
		}
		if lastErr != nil && usedCandidate.PeerID == "" {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		if err := store.PutPiece(pieceIndex, data); err != nil {
			return fmt.Errorf("store piece %d: %w", pieceIndex, err)
		}
		if err := syncTrackerProgress(logger, discovery, store); err != nil {
			logger.Error("tracker_progress_sync_failed",
				"contentId", manifest.ContentID,
				"pieceIndex", pieceIndex,
				"tracker", discovery.trackerURL,
				"error", err.Error(),
			)
		}
		reportTrackerTransferPath(logger, discovery, manifest.ContentID, usedCandidate)
		if runtime := store.RuntimeStats(); runtime != nil {
			_ = runtime.RecordDownload(int64(len(data)), transferPathForPeer(usedCandidate.PeerID), usedCandidate.PeerID)
		}
		logger.Info("piece_downloaded",
			"contentId", manifest.ContentID,
			"pieceIndex", pieceIndex,
			"bytes", len(data),
			"peer", usedCandidate.PeerID,
		)
		return nil
	}

	return fmt.Errorf("failed to download piece %d after retries", pieceIndex)
}

func fetchPieceFromCandidate(candidate scheduler.PeerCandidate, contentID string, pieceIndex int) ([]byte, error) {
	switch candidate.Transport {
	case "udp":
		return p2pnet.NewUDPClient(candidate.Addr, 4*time.Second).FetchPiece(contentID, pieceIndex)
	default:
		addr := candidate.Addr
		if addr == "" {
			addr = candidate.PeerID
		}
		return p2pnet.NewClient(addr, 10*time.Second).FetchPiece(contentID, pieceIndex)
	}
}

func transferErrorKind(candidate scheduler.PeerCandidate, err error) string {
	if err == nil {
		return ""
	}
	if candidate.Transport == "udp" && p2pnet.IsUDPTimeout(err) {
		return "udp_timeout"
	}
	return "generic"
}

func pieceAttemptCandidates(pieceIndex int, selected scheduler.PeerCandidate, peerCandidates []scheduler.PeerCandidate) []scheduler.PeerCandidate {
	if selected.Transport != "udp" {
		return []scheduler.PeerCandidate{selected}
	}

	attempts := []scheduler.PeerCandidate{selected}
	alternatives := make([]scheduler.PeerCandidate, 0, len(peerCandidates))
	for _, candidate := range peerCandidates {
		if candidate.PeerID == selected.PeerID || candidate.Transport != "udp" {
			continue
		}
		if !core.ContainsPiece(candidate.HaveRanges, pieceIndex) {
			continue
		}
		alternatives = append(alternatives, candidate)
	}
	sort.Slice(alternatives, func(i, j int) bool {
		if alternatives[i].Score != alternatives[j].Score {
			return alternatives[i].Score > alternatives[j].Score
		}
		if alternatives[i].PendingCount != alternatives[j].PendingCount {
			return alternatives[i].PendingCount < alternatives[j].PendingCount
		}
		return alternatives[i].PeerID < alternatives[j].PeerID
	})
	if len(alternatives) > 2 {
		alternatives = alternatives[:2]
	}
	return append(attempts, alternatives...)
}

func reportTrackerTransferPath(logger *logging.Logger, discovery peerDiscoveryOptions, contentID string, candidate scheduler.PeerCandidate) {
	if strings.TrimSpace(discovery.trackerURL) == "" || strings.TrimSpace(candidate.PeerID) == "" {
		return
	}
	transport := strings.ToLower(strings.TrimSpace(candidate.Transport))
	if transport != "udp" && transport != "tcp" {
		return
	}
	client := tracker.NewClient(discovery.trackerURL)
	if err := client.ReportTransferPath(context.Background(), candidate.PeerID, contentID, transport); err != nil {
		logger.Error("tracker_transfer_report_failed",
			"contentId", contentID,
			"peer", candidate.PeerID,
			"transport", transport,
			"tracker", discovery.trackerURL,
			"error", err.Error(),
		)
	}
}

func reserveNextPiece(manifest *core.ContentManifest, store *core.PieceStore, state *downloadPieceState, peerCandidates []scheduler.PeerCandidate) (int, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()

	chooser := scheduler.Scheduler{}
	completed := store.CompletedPieceMap()
	pieceIndex, ok := chooser.ChoosePiece(len(manifest.Pieces), peerCandidates, completed, state.inProgress)
	if !ok {
		return 0, false
	}
	state.inProgress[pieceIndex] = true
	return pieceIndex, true
}

func allPiecesCompleted(manifest *core.ContentManifest, store *core.PieceStore) bool {
	for _, piece := range manifest.Pieces {
		if !store.HasPiece(piece.Index) {
			return false
		}
	}
	return true
}
