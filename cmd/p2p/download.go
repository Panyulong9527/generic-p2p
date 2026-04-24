package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/logging"
	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/scheduler"
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

				peerCandidates, err := collectDynamicPeerCandidates(logger, discovery, peerHealth, discoveryCache, udpProbes, udpProbeRequests, nil, peerLoad, peerUsage)
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

				err = downloadSinglePiece(logger, manifest, store, discovery, pieceIndex, id, peerHealth, discoveryCache, udpProbes, udpProbeRequests, peerLoad, peerUsage)

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

func downloadSinglePiece(logger *logging.Logger, manifest *core.ContentManifest, store *core.PieceStore, discovery peerDiscoveryOptions, pieceIndex int, workerID int, peerHealth *peerHealthState, discoveryCache *peerDiscoveryCache, udpProbes *udpProbeCache, udpProbeRequests *udpProbeRequestCache, peerLoad *peerLoadState, peerUsage *peerUsageState) error {
	chooser := scheduler.Scheduler{}
	excluded := make(map[string]bool)

	for attempt := 0; attempt < 5; attempt++ {
		peerCandidates, err := collectDynamicPeerCandidates(logger, discovery, peerHealth, discoveryCache, udpProbes, udpProbeRequests, excluded, peerLoad, peerUsage)
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

		peerLoad.Acquire(selected.PeerID)
		if runtime := store.RuntimeStats(); runtime != nil {
			_ = runtime.StartDownload(pieceIndex, selected.PeerID, workerID, time.Now())
		}
		peerUsage.RecordAssignment(selected.PeerID)
		data, err := fetchPieceFromCandidate(selected, manifest.ContentID, pieceIndex)
		peerLoad.Release(selected.PeerID)
		if runtime := store.RuntimeStats(); runtime != nil {
			_ = runtime.FinishDownload(pieceIndex)
		}
		if err != nil {
			cooldown := peerHealth.MarkFailure(selected.PeerID, time.Now())
			logger.Error("piece_download_failed",
				"contentId", manifest.ContentID,
				"pieceIndex", pieceIndex,
				"peer", selected.PeerID,
				"attempt", attempt+1,
				"cooldownMs", cooldown.Milliseconds(),
				"error", err.Error(),
			)
			excluded[selected.PeerID] = true
			time.Sleep(150 * time.Millisecond)
			continue
		}
		peerHealth.MarkSuccess(selected.PeerID)
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
		if runtime := store.RuntimeStats(); runtime != nil {
			_ = runtime.RecordDownload(int64(len(data)), transferPathForPeer(selected.PeerID), selected.PeerID)
		}
		logger.Info("piece_downloaded",
			"contentId", manifest.ContentID,
			"pieceIndex", pieceIndex,
			"bytes", len(data),
			"peer", selected.PeerID,
		)
		return nil
	}

	return fmt.Errorf("failed to download piece %d after retries", pieceIndex)
}

func fetchPieceFromCandidate(candidate scheduler.PeerCandidate, contentID string, pieceIndex int) ([]byte, error) {
	switch candidate.Transport {
	case "udp":
		return p2pnet.NewUDPClient(candidate.Addr, 10*time.Second).FetchPiece(contentID, pieceIndex)
	default:
		addr := candidate.Addr
		if addr == "" {
			addr = candidate.PeerID
		}
		return p2pnet.NewClient(addr, 10*time.Second).FetchPiece(contentID, pieceIndex)
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
