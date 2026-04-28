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

type udpChunkProgressSample struct {
	RequestedChunks int
	ReceivedChunks  int
	TotalChunks     int
	Duration        time.Duration
	Completed       bool
	RecordedAt      time.Time
}

var udpChunkProgressState = struct {
	mu      sync.Mutex
	entries map[string]udpChunkProgressSample
}{
	entries: make(map[string]udpChunkProgressSample),
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
	udpBurstReports := newUDPBurstProfileReportCache()
	udpDecisionReports := newUDPDecisionReportCache()
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
					syncRuntimeUDPBurstProfiles(logger, store, discovery, manifest.ContentID, udpBurstReports)
				}

				pieceIndex, ok := reserveNextPiece(manifest, store, state, peerCandidates)
				if !ok {
					if allPiecesCompleted(manifest, store) {
						return
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				err = downloadSinglePiece(logger, manifest, store, discovery, pieceIndex, id, peerHealth, discoveryCache, trackerStatus, udpProbes, udpProbeRequests, udpBurstReports, udpDecisionReports, peerLoad, peerUsage)

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

func downloadSinglePiece(logger *logging.Logger, manifest *core.ContentManifest, store *core.PieceStore, discovery peerDiscoveryOptions, pieceIndex int, workerID int, peerHealth *peerHealthState, discoveryCache *peerDiscoveryCache, trackerStatus *trackerStatusCache, udpProbes *udpProbeCache, udpProbeRequests *udpProbeRequestCache, udpBurstReports *udpBurstProfileReportCache, udpDecisionReports *udpDecisionReportCache, peerLoad *peerLoadState, peerUsage *peerUsageState) error {
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
			syncRuntimeUDPBurstProfiles(logger, store, discovery, manifest.ContentID, udpBurstReports)
		}

		selected, ok := chooser.ChoosePeer(pieceIndex, peerCandidates)
		if !ok {
			if attempt == 4 {
				return fmt.Errorf("no peer has piece %d", pieceIndex)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		decision := recordSelectionDecision(store, pieceIndex, selected, peerCandidates)
		reportTrackerUDPDecision(logger, discovery, manifest.ContentID, decision, udpDecisionReports)
		attemptCandidates := pieceAttemptCandidates(manifest.ContentID, pieceIndex, selected, peerCandidates)
		var data []byte
		var usedCandidate scheduler.PeerCandidate
		var lastErr error
		for burstIndex, candidate := range attemptCandidates {
			peerLoad.Acquire(candidate.PeerID)
			if runtime := store.RuntimeStats(); runtime != nil {
				_ = runtime.StartDownload(pieceIndex, candidate.PeerID, workerID, time.Now())
			}
			peerUsage.RecordAssignment(candidate.PeerID)
			data, lastErr = fetchPieceFromCandidate(candidate, manifest.ContentID, pieceIndex, discovery.selfUDPListenAddr)
			peerLoad.Release(candidate.PeerID)
			if runtime := store.RuntimeStats(); runtime != nil {
				_ = runtime.FinishDownload(pieceIndex)
			}
			if lastErr != nil {
				if candidate.Transport == "udp" {
					recordUDPBurstOutcome(manifest.ContentID, candidate.PeerID, normalizedBurstProfile(candidate.BurstProfile), "piece", false, time.Now())
				}
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
			if candidate.Transport == "udp" {
				recordUDPBurstOutcome(manifest.ContentID, candidate.PeerID, normalizedBurstProfile(candidate.BurstProfile), "piece", true, time.Now())
				p2pnet.RememberRecentUDPSuccess(manifest.ContentID, candidate.Addr, time.Now())
			}
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

func fetchPieceFromCandidate(candidate scheduler.PeerCandidate, contentID string, pieceIndex int, selfUDPListenAddr string) ([]byte, error) {
	switch candidate.Transport {
	case "udp":
		return p2pnet.NewUDPClient(candidate.Addr, udpPieceTimeout(contentID, candidate)).
			WithLocalAddr(selfUDPListenAddr).
			WithChunkWindow(udpPieceChunkWindowForCandidate(contentID, candidate)).
			WithChunkRoundTimeout(udpPieceChunkRoundTimeoutForCandidate(contentID, candidate)).
			WithPieceRoundObserver(func(stats p2pnet.UDPPieceRoundStats) {
				recordUDPChunkProgress(contentID, candidate.PeerID, stats, time.Now())
			}).
			FetchPiece(contentID, pieceIndex)
	default:
		addr := candidate.Addr
		if addr == "" {
			addr = candidate.PeerID
		}
		return p2pnet.NewClient(addr, 10*time.Second).FetchPiece(contentID, pieceIndex)
	}
}

func udpPieceChunkWindowForCandidate(contentID string, candidate scheduler.PeerCandidate) int {
	window := udpPieceChunkWindowForProfile(candidate.BurstProfile)
	stage := currentUDPBurstStageForPeer(contentID, candidate.PeerID, time.Now())
	window = stageAdjustedUDPPieceChunkWindow(window, stage)
	window = decisionRiskAdjustedUDPPieceChunkWindow(window, candidate.UDPDecisionRisk)
	window = progressAdjustedUDPPieceChunkWindow(window, contentID, candidate.PeerID, time.Now())
	if candidate.UDPPublicMapped && !isSuppressedDecisionRisk(candidate.UDPDecisionRisk) && window < 8 {
		window++
	}
	if window < 1 {
		return 1
	}
	if window > 8 {
		return 8
	}
	return window
}

func udpPieceChunkRoundTimeoutForCandidate(contentID string, candidate scheduler.PeerCandidate) time.Duration {
	timeout := udpPieceChunkRoundTimeoutForProfile(candidate.BurstProfile)
	stage := currentUDPBurstStageForPeer(contentID, candidate.PeerID, time.Now())
	timeout = stageAdjustedUDPPieceChunkRoundTimeout(timeout, stage)
	timeout = decisionRiskAdjustedUDPPieceChunkRoundTimeout(timeout, candidate.UDPDecisionRisk)
	timeout = progressAdjustedUDPPieceChunkRoundTimeout(timeout, contentID, candidate.PeerID, time.Now())
	if candidate.UDPPublicMapped && !isSuppressedDecisionRisk(candidate.UDPDecisionRisk) {
		timeout -= 150 * time.Millisecond
	}
	if timeout < 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	if timeout > 2200*time.Millisecond {
		return 2200 * time.Millisecond
	}
	return timeout
}

func udpPieceChunkWindowForProfile(profile string) int {
	switch strings.TrimSpace(profile) {
	case "warm":
		return 5
	case "aggressive":
		return 3
	default:
		return 4
	}
}

func stageAdjustedUDPPieceChunkWindow(base int, stage string) int {
	switch strings.TrimSpace(stage) {
	case "probe":
		if base > 1 {
			return base - 1
		}
		return 1
	case "piece":
		if base < 8 {
			return base + 1
		}
		return 8
	default:
		return base
	}
}

func udpPieceChunkRoundTimeoutForProfile(profile string) time.Duration {
	switch strings.TrimSpace(profile) {
	case "warm":
		return 700 * time.Millisecond
	case "aggressive":
		return 1400 * time.Millisecond
	default:
		return time.Second
	}
}

func stageAdjustedUDPPieceChunkRoundTimeout(base time.Duration, stage string) time.Duration {
	switch strings.TrimSpace(stage) {
	case "probe":
		if base > 250*time.Millisecond {
			return base - 250*time.Millisecond
		}
		return 250 * time.Millisecond
	case "piece":
		if base < 1900*time.Millisecond {
			return base + 300*time.Millisecond
		}
		return 2200 * time.Millisecond
	default:
		return base
	}
}

func decisionRiskAdjustedUDPPieceChunkRoundTimeout(base time.Duration, risk string) time.Duration {
	switch strings.TrimSpace(risk) {
	case "low":
		if base > 400*time.Millisecond {
			return base - 300*time.Millisecond
		}
		return 250 * time.Millisecond
	case "warn":
		if base > 300*time.Millisecond {
			return base - 150*time.Millisecond
		}
		return 250 * time.Millisecond
	case "stable":
		if base < 2*time.Second {
			return base + 200*time.Millisecond
		}
		return 2200 * time.Millisecond
	default:
		return base
	}
}

func decisionRiskAdjustedUDPPieceChunkWindow(base int, risk string) int {
	switch strings.TrimSpace(risk) {
	case "low":
		if base > 2 {
			return base - 2
		}
		return 1
	case "warn":
		if base > 1 {
			return base - 1
		}
		return 1
	case "stable":
		if base < 8 {
			return base + 1
		}
		return 8
	default:
		return base
	}
}

func progressAdjustedUDPPieceChunkWindow(base int, contentID string, peerID string, now time.Time) int {
	sample, ok := recentUDPChunkProgress(contentID, peerID, now)
	if !ok {
		return base
	}
	switch {
	case sample.Completed && sample.Duration <= 900*time.Millisecond:
		if base < 8 {
			return base + 1
		}
	case sample.RequestedChunks > 0 && sample.ReceivedChunks*2 < sample.RequestedChunks:
		if base > 1 {
			return base - 1
		}
	case sample.RequestedChunks >= 3 && sample.ReceivedChunks >= sample.RequestedChunks-1:
		if base < 8 {
			return base + 1
		}
	}
	return base
}

func progressAdjustedUDPPieceChunkRoundTimeout(base time.Duration, contentID string, peerID string, now time.Time) time.Duration {
	sample, ok := recentUDPChunkProgress(contentID, peerID, now)
	if !ok {
		return base
	}
	switch {
	case sample.Completed && sample.Duration+150*time.Millisecond < base:
		return base - 150*time.Millisecond
	case sample.RequestedChunks > 0 && sample.ReceivedChunks == 0:
		return base + 250*time.Millisecond
	case sample.RequestedChunks > 0 && sample.ReceivedChunks*2 < sample.RequestedChunks:
		return base + 150*time.Millisecond
	}
	return base
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

func pieceAttemptCandidates(contentID string, pieceIndex int, selected scheduler.PeerCandidate, peerCandidates []scheduler.PeerCandidate) []scheduler.PeerCandidate {
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
		if udpDecisionRiskPriority(alternatives[i].UDPDecisionRisk) != udpDecisionRiskPriority(alternatives[j].UDPDecisionRisk) {
			return udpDecisionRiskPriority(alternatives[i].UDPDecisionRisk) < udpDecisionRiskPriority(alternatives[j].UDPDecisionRisk)
		}
		if alternatives[i].PendingCount != alternatives[j].PendingCount {
			return alternatives[i].PendingCount < alternatives[j].PendingCount
		}
		return alternatives[i].PeerID < alternatives[j].PeerID
	})
	maxAlternatives := udpAttemptBudget(contentID, selected) - 1
	if maxAlternatives < 0 {
		maxAlternatives = 0
	}
	if len(alternatives) > maxAlternatives {
		alternatives = alternatives[:maxAlternatives]
	}
	return append(attempts, alternatives...)
}

func udpAttemptBudget(contentID string, selected scheduler.PeerCandidate) int {
	base := udpAttemptBudgetForProfile(selected.BurstProfile)
	stage := currentUDPBurstStageForPeer(contentID, selected.PeerID, time.Now())
	base = stageAdjustedUDPAttemptBudget(base, stage)
	return decisionRiskAdjustedUDPAttemptBudget(base, selected.UDPDecisionRisk)
}

func udpAttemptBudgetForProfile(profile string) int {
	switch strings.TrimSpace(profile) {
	case "aggressive":
		return 4
	case "warm":
		return 2
	default:
		return 3
	}
}

func stageAdjustedUDPAttemptBudget(base int, stage string) int {
	switch strings.TrimSpace(stage) {
	case "probe":
		if base > 1 {
			return base - 1
		}
		return 1
	case "have":
		return base
	case "piece":
		if base < 5 {
			return base + 1
		}
		return 5
	default:
		return base
	}
}

func decisionRiskAdjustedUDPAttemptBudget(base int, risk string) int {
	switch strings.TrimSpace(risk) {
	case "low":
		if base > 2 {
			return base - 2
		}
		return 1
	case "warn":
		if base > 1 {
			return base - 1
		}
		return 1
	case "recovering":
		if base < 5 {
			return base + 1
		}
		return 5
	case "stable":
		if base < 5 {
			return base + 1
		}
		return 5
	default:
		return base
	}
}

func currentUDPBurstStageForPeer(contentID string, peerID string, now time.Time) string {
	stats, ok := udpBurstStats(contentID, peerID, now)
	if !ok {
		return ""
	}
	return currentUDPBurstStage(stats)
}

func udpPieceTimeout(contentID string, selected scheduler.PeerCandidate) time.Duration {
	base := udpPieceTimeoutForProfile(selected.BurstProfile)
	stage := currentUDPBurstStageForPeer(contentID, selected.PeerID, time.Now())
	base = stageAdjustedUDPPieceTimeout(base, stage)
	return decisionRiskAdjustedUDPPieceTimeout(base, selected.UDPDecisionRisk)
}

func udpPieceTimeoutForProfile(profile string) time.Duration {
	switch strings.TrimSpace(profile) {
	case "aggressive":
		return 5500 * time.Millisecond
	case "warm":
		return 3500 * time.Millisecond
	default:
		return 4500 * time.Millisecond
	}
}

func stageAdjustedUDPPieceTimeout(base time.Duration, stage string) time.Duration {
	switch strings.TrimSpace(stage) {
	case "probe":
		if base > 900*time.Millisecond {
			return base - 900*time.Millisecond
		}
		return base
	case "have":
		return base
	case "piece":
		return base + 1200*time.Millisecond
	default:
		return base
	}
}

func decisionRiskAdjustedUDPPieceTimeout(base time.Duration, risk string) time.Duration {
	switch strings.TrimSpace(risk) {
	case "low":
		return clampUDPPieceTimeout(base-1200*time.Millisecond, 2200*time.Millisecond)
	case "warn":
		return clampUDPPieceTimeout(base-600*time.Millisecond, 2200*time.Millisecond)
	case "recovering":
		return base + 600*time.Millisecond
	case "stable":
		return base + 900*time.Millisecond
	default:
		return base
	}
}

func clampUDPPieceTimeout(timeout time.Duration, min time.Duration) time.Duration {
	if timeout < min {
		return min
	}
	return timeout
}

func normalizedBurstProfile(profile string) string {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return "default"
	}
	return profile
}

func recordUDPChunkProgress(contentID string, peerID string, stats p2pnet.UDPPieceRoundStats, now time.Time) {
	if strings.TrimSpace(contentID) == "" || strings.TrimSpace(peerID) == "" {
		return
	}
	if stats.RequestedChunks <= 0 {
		return
	}
	udpChunkProgressState.mu.Lock()
	defer udpChunkProgressState.mu.Unlock()
	udpChunkProgressState.entries[contentID+"|"+peerID] = udpChunkProgressSample{
		RequestedChunks: stats.RequestedChunks,
		ReceivedChunks:  stats.ReceivedChunks,
		TotalChunks:     stats.TotalChunks,
		Duration:        stats.Duration,
		Completed:       stats.Completed,
		RecordedAt:      now,
	}
}

func recentUDPChunkProgress(contentID string, peerID string, now time.Time) (udpChunkProgressSample, bool) {
	if strings.TrimSpace(contentID) == "" || strings.TrimSpace(peerID) == "" {
		return udpChunkProgressSample{}, false
	}
	key := contentID + "|" + peerID
	udpChunkProgressState.mu.Lock()
	defer udpChunkProgressState.mu.Unlock()
	sample, ok := udpChunkProgressState.entries[key]
	if !ok {
		return udpChunkProgressSample{}, false
	}
	if now.Sub(sample.RecordedAt) > 20*time.Second {
		delete(udpChunkProgressState.entries, key)
		return udpChunkProgressSample{}, false
	}
	return sample, true
}

func recordSelectionDecision(store *core.PieceStore, pieceIndex int, selected scheduler.PeerCandidate, peerCandidates []scheduler.PeerCandidate) core.SelectionDecision {
	runtime := store.RuntimeStats()
	contentID := store.Manifest().ContentID
	decision := core.SelectionDecision{
		PieceIndex:              pieceIndex,
		SelectedPeerID:          selected.PeerID,
		SelectedTransport:       selected.Transport,
		SelectedScore:           selected.Score,
		SelectedBurstProfile:    normalizedBurstProfile(selected.BurstProfile),
		SelectedLastStage:       currentUDPBurstStageForPeer(contentID, selected.PeerID, time.Now()),
		SelectedUDPDecisionRisk: strings.TrimSpace(selected.UDPDecisionRisk),
		Reason:                  selectionReason(pieceIndex, selected, peerCandidates),
		RecordedAt:              time.Now().Format(time.RFC3339),
	}
	if selected.Transport == "udp" {
		decision.SelectedUDPBudget = udpAttemptBudget(contentID, selected)
		decision.SelectedUDPTimeoutMs = udpPieceTimeout(contentID, selected).Milliseconds()
	}
	if topUDP, ok := topUDPCandidateForPiece(pieceIndex, peerCandidates); ok {
		decision.TopUDPPeerID = topUDP.PeerID
		decision.TopUDPScore = topUDP.Score
	}
	if runtime != nil {
		_ = runtime.RecordSelectionDecision(decision)
	}
	return decision
}

func syncRuntimeUDPBurstProfiles(logger *logging.Logger, store *core.PieceStore, discovery peerDiscoveryOptions, contentID string, reportCache *udpBurstProfileReportCache) {
	runtime := store.RuntimeStats()
	if runtime == nil {
		return
	}
	profiles := currentUDPBurstProfiles(contentID, time.Now())
	_ = runtime.SetUDPBurstProfiles(profiles)
	reportTrackerUDPBurstProfiles(logger, discovery, contentID, profiles, reportCache)
}

func reportTrackerUDPBurstProfiles(logger *logging.Logger, discovery peerDiscoveryOptions, contentID string, profiles []core.UDPBurstProfileStatus, reportCache *udpBurstProfileReportCache) {
	if strings.TrimSpace(discovery.trackerURL) == "" || len(profiles) == 0 {
		return
	}
	client := tracker.NewClient(discovery.trackerURL)
	for _, profile := range profiles {
		if strings.TrimSpace(profile.PeerID) == "" || strings.TrimSpace(profile.Profile) == "" {
			continue
		}
		key := contentID + "|" + profile.PeerID
		fingerprint := profile.Profile + "|" + profile.LastStage + "|" + profile.LastSuccessAt + "|" + profile.LastFailureAt + "|" + fmt.Sprintf("%d", profile.FailureCount)
		if !reportCache.ShouldReport(key, fingerprint) {
			continue
		}
		lastOutcome := "unknown"
		lastOutcomeAt := ""
		if profile.LastFailureAt != "" && (profile.LastSuccessAt == "" || profile.LastFailureAt > profile.LastSuccessAt) {
			lastOutcome = "failure"
			lastOutcomeAt = profile.LastFailureAt
		} else if profile.LastSuccessAt != "" {
			lastOutcome = "success"
			lastOutcomeAt = profile.LastSuccessAt
		}
		if err := client.ReportUDPBurstProfile(context.Background(), profile.PeerID, contentID, profile.Profile, lastOutcome, profile.LastStage, profile.FailureCount, lastOutcomeAt); err != nil {
			logger.Error("tracker_udp_burst_profile_report_failed",
				"contentId", contentID,
				"peerId", profile.PeerID,
				"profile", profile.Profile,
				"error", err.Error(),
			)
		}
	}
}

func reportTrackerUDPDecision(logger *logging.Logger, discovery peerDiscoveryOptions, contentID string, decision core.SelectionDecision, reportCache *udpDecisionReportCache) {
	if strings.TrimSpace(discovery.trackerURL) == "" || decision.SelectedTransport != "udp" || strings.TrimSpace(decision.SelectedPeerID) == "" {
		return
	}
	key := contentID + "|" + decision.SelectedPeerID
	fingerprint := decision.SelectedBurstProfile + "|" + decision.SelectedLastStage + "|" + fmt.Sprintf("%d|%d|%.3f|%s", decision.SelectedUDPBudget, decision.SelectedUDPTimeoutMs, decision.SelectedScore, decision.Reason)
	if !reportCache.ShouldReport(key, fingerprint) {
		return
	}
	client := tracker.NewClient(discovery.trackerURL)
	if err := client.ReportUDPDecision(context.Background(), decision.SelectedPeerID, contentID, decision.SelectedBurstProfile, decision.SelectedLastStage, decision.SelectedUDPBudget, decision.SelectedUDPTimeoutMs, decision.SelectedScore, decision.Reason); err != nil {
		logger.Error("tracker_udp_decision_report_failed",
			"contentId", contentID,
			"peerId", decision.SelectedPeerID,
			"profile", decision.SelectedBurstProfile,
			"stage", decision.SelectedLastStage,
			"error", err.Error(),
		)
	}
}

func selectionReason(pieceIndex int, selected scheduler.PeerCandidate, peerCandidates []scheduler.PeerCandidate) string {
	if selected.Transport == "udp" {
		if selected.UDPPublicMapped && selectedByPublicMappedPreference(pieceIndex, selected, peerCandidates) {
			return "selected_udp_public_mapped_close_score"
		}
		switch strings.TrimSpace(selected.UDPDecisionRisk) {
		case "low":
			return "selected_udp_despite_low_value_risk"
		case "warn":
			return "selected_udp_under_watch_risk"
		case "recovering":
			return "selected_udp_recovering_path"
		case "stable":
			return "selected_udp_stable_path"
		}
		return "selected_udp_best_score"
	}
	if topUDP, ok := topUDPCandidateForPiece(pieceIndex, peerCandidates); ok {
		switch strings.TrimSpace(topUDP.UDPDecisionRisk) {
		case "low":
			return "selected_tcp_over_low_value_udp"
		case "warn":
			return "selected_tcp_over_watch_udp"
		}
		if topUDP.Score < selected.Score {
			return "selected_tcp_over_lower_udp_score"
		}
		return "selected_tcp_over_udp_candidate"
	}
	return "selected_tcp_no_udp_candidate"
}

func selectedByPublicMappedPreference(pieceIndex int, selected scheduler.PeerCandidate, peerCandidates []scheduler.PeerCandidate) bool {
	if selected.Transport != "udp" || !selected.UDPPublicMapped || isSuppressedDecisionRisk(selected.UDPDecisionRisk) {
		return false
	}
	margin := publicMappedDecisionMargin(selected.UDPDecisionRisk)
	for _, candidate := range peerCandidates {
		if candidate.Transport != "tcp" {
			continue
		}
		if pieceIndex >= 0 && !core.ContainsPiece(candidate.HaveRanges, pieceIndex) {
			continue
		}
		if candidate.Score >= selected.Score-margin {
			return true
		}
	}
	return false
}

func isSuppressedDecisionRisk(risk string) bool {
	switch strings.TrimSpace(risk) {
	case "low", "warn":
		return true
	default:
		return false
	}
}

func publicMappedDecisionMargin(risk string) float64 {
	switch strings.TrimSpace(risk) {
	case "stable":
		return 0.28
	case "recovering":
		return 0.18
	default:
		return 0.12
	}
}

func topUDPCandidateForPiece(pieceIndex int, peerCandidates []scheduler.PeerCandidate) (scheduler.PeerCandidate, bool) {
	var (
		best scheduler.PeerCandidate
		ok   bool
	)
	for _, candidate := range peerCandidates {
		if candidate.Transport != "udp" {
			continue
		}
		if pieceIndex >= 0 && !core.ContainsPiece(candidate.HaveRanges, pieceIndex) {
			continue
		}
		if !ok || candidate.Score > best.Score || (candidate.Score == best.Score && candidate.PendingCount < best.PendingCount) || (candidate.Score == best.Score && candidate.PendingCount == best.PendingCount && candidate.PeerID < best.PeerID) {
			best = candidate
			ok = true
		}
	}
	return best, ok
}

func udpDecisionRiskPriority(risk string) int {
	switch strings.TrimSpace(risk) {
	case "stable":
		return 0
	case "recovering":
		return 1
	case "":
		return 2
	case "warn":
		return 3
	case "low":
		return 4
	default:
		return 2
	}
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
