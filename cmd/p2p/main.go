package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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

type peerDiscoveryOptions struct {
	contentID      string
	explicitPeer   string
	explicitPeers  string
	lanEnabled     bool
	lanAddr        string
	trackerURL     string
	selfListenAddr string
}

type downloadPieceState struct {
	mu         sync.Mutex
	inProgress map[int]bool
}

func main() {
	logger := logging.NewJSONLogger(os.Stdout, logging.ParseLevel(os.Getenv("P2P_LOG_LEVEL")))

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	command := os.Args[1]
	args := os.Args[2:]

	var err error
	switch command {
	case "share":
		err = runShare(logger, args)
	case "tracker":
		err = runTracker(logger, args)
	case "serve":
		err = runServe(logger, args)
	case "get":
		err = runGet(logger, args)
	case "status":
		err = runStatus(args)
	default:
		err = fmt.Errorf("unknown command %q", command)
	}

	if err != nil {
		logger.Error("command_failed", "command", command, "error", err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func runShare(logger *logging.Logger, args []string) error {
	fs := flag.NewFlagSet("share", flag.ContinueOnError)
	path := fs.String("path", "", "file to share")
	pieceSize := fs.Int64("piece-size", core.DefaultPieceSize, "piece size in bytes")
	dataDir := fs.String("data-dir", ".p2p", "directory for manifest and piece metadata")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("share requires --path")
	}
	if *pieceSize <= 0 {
		return errors.New("piece size must be greater than zero")
	}

	manifest, err := core.BuildManifestFromFile(*path, *pieceSize)
	if err != nil {
		return err
	}

	shareDir := filepath.Join(*dataDir, manifest.ContentID)
	if err := os.MkdirAll(shareDir, 0o755); err != nil {
		return err
	}
	manifestPath := filepath.Join(shareDir, "manifest.json")
	if err := core.WriteManifestFile(manifestPath, manifest); err != nil {
		return err
	}

	logger.Info("share_ready",
		"path", *path,
		"contentId", manifest.ContentID,
		"pieces", len(manifest.Pieces),
		"pieceSize", manifest.PieceSize,
		"manifestPath", manifestPath,
	)

	out, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func runGet(logger *logging.Logger, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "path to manifest.json")
	storeDir := fs.String("store-dir", ".p2p-store", "piece store directory")
	out := fs.String("out", "", "output file path")
	peerAddr := fs.String("peer", "", "peer address such as 127.0.0.1:9001")
	peerList := fs.String("peers", "", "comma separated peer addresses")
	listen := fs.String("listen", "", "optional listen address for serving completed pieces while downloading")
	seedAfterDownload := fs.Bool("seed-after-download", false, "keep serving after download completes when --listen is set")
	lan := fs.Bool("lan", false, "enable LAN discovery")
	lanAddr := fs.String("lan-addr", p2pnet.DefaultLANDiscoveryAddr, "LAN discovery UDP target/listen address")
	trackerURL := fs.String("tracker", "", "tracker base URL such as http://127.0.0.1:7000")
	workers := fs.Int("workers", 2, "number of concurrent download workers")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("get requires --manifest")
	}
	if *out == "" {
		return errors.New("get requires --out")
	}

	manifest, err := core.ReadManifestFile(*manifestPath)
	if err != nil {
		return err
	}

	store, err := core.OpenPieceStore(filepath.Join(*storeDir, manifest.ContentID), manifest)
	if err != nil {
		return err
	}
	defer store.Close()

	if *listen != "" {
		server := p2pnet.NewServer(*listen, p2pnet.StoreContentSource{
			Store: store,
		})
		server.OnPieceServed = func(bytes int64, path string, peerID string) {
			if runtime := store.RuntimeStats(); runtime != nil {
				_ = runtime.RecordUpload(bytes, path, peerID)
			}
		}
		go func() {
			if serveErr := server.Listen(context.Background()); serveErr != nil {
				logger.Error("downloader_serve_failed", "listen", *listen, "error", serveErr.Error())
			}
		}()
		logger.Info("downloader_serve_ready",
			"contentId", manifest.ContentID,
			"listen", *listen,
			"completedRanges", store.CompletedRanges(),
		)

		if *lan {
			go func() {
				err := p2pnet.AnnounceLAN(context.Background(), *lanAddr, time.Second, func() p2pnet.LANAnnouncement {
					return p2pnet.BuildLANAnnouncement(
						*listen,
						*listen,
						[]p2pnet.LANContent{
							{
								ContentID:  manifest.ContentID,
								HaveRanges: store.CompletedRanges(),
							},
						},
					)
				})
				if err != nil {
					logger.Error("lan_announce_failed", "lanAddr", *lanAddr, "error", err.Error())
				}
			}()
			logger.Info("lan_announce_ready", "contentId", manifest.ContentID, "listen", *listen, "lanAddr", *lanAddr)
		}

		if *trackerURL != "" {
			go func() {
				startTrackerSyncLoop(logger, *trackerURL, *listen, manifest.ContentID, time.Second, store.CompletedRanges)
			}()
			logger.Info("tracker_sync_ready", "contentId", manifest.ContentID, "tracker", *trackerURL, "peer", *listen)
		}
	}

	peerAddrs := collectPeerAddrs(*peerAddr, *peerList)
	if *lan {
		discoveredAddrs, err := discoverLANPeers(logger, manifest.ContentID, *lanAddr, *listen)
		if err != nil {
			return err
		}
		peerAddrs = appendUnique(peerAddrs, discoveredAddrs...)
	}
	if *trackerURL != "" {
		discoveredAddrs, err := discoverTrackerPeers(logger, manifest.ContentID, *trackerURL, *listen)
		if err != nil {
			return err
		}
		peerAddrs = appendUnique(peerAddrs, discoveredAddrs...)
	}

	discoveryOptions := peerDiscoveryOptions{
		contentID:      manifest.ContentID,
		explicitPeer:   *peerAddr,
		explicitPeers:  *peerList,
		lanEnabled:     *lan,
		lanAddr:        *lanAddr,
		trackerURL:     *trackerURL,
		selfListenAddr: *listen,
	}

	if *workers <= 0 {
		return errors.New("workers must be greater than zero")
	}

	if err := downloadPieces(logger, manifest, store, discoveryOptions, *workers); err != nil {
		return err
	}

	status := store.Status()
	logger.Info("get_ready",
		"contentId", manifest.ContentID,
		"completedPieces", status.CompletedPieces,
		"totalPieces", status.TotalPieces,
		"progress", status.Progress(),
	)

	if status.CompletedPieces == status.TotalPieces && status.TotalPieces > 0 {
		if err := store.AssembleTo(*out); err != nil {
			return err
		}
		logger.Info("content_assembled", "contentId", manifest.ContentID, "out", *out)
	}

	statusJSON, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(statusJSON))

	if *seedAfterDownload {
		if *listen == "" {
			return errors.New("--seed-after-download requires --listen")
		}
		logger.Info("seeding_after_download", "contentId", manifest.ContentID, "listen", *listen)
		select {}
	}
	return nil
}

func downloadPieces(logger *logging.Logger, manifest *core.ContentManifest, store *core.PieceStore, discovery peerDiscoveryOptions, workers int) error {
	state := &downloadPieceState{
		inProgress: make(map[int]bool),
	}
	peerHealth := newPeerHealthState()
	discoveryCache := newPeerDiscoveryCache(400 * time.Millisecond)
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

				peerCandidates, err := collectDynamicPeerCandidates(logger, discovery, peerHealth, discoveryCache, nil, peerLoad, peerUsage)
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

				err = downloadSinglePiece(logger, manifest, store, discovery, pieceIndex, id, peerHealth, discoveryCache, peerLoad, peerUsage)

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

func downloadSinglePiece(logger *logging.Logger, manifest *core.ContentManifest, store *core.PieceStore, discovery peerDiscoveryOptions, pieceIndex int, workerID int, peerHealth *peerHealthState, discoveryCache *peerDiscoveryCache, peerLoad *peerLoadState, peerUsage *peerUsageState) error {
	chooser := scheduler.Scheduler{}
	excluded := make(map[string]bool)

	for attempt := 0; attempt < 5; attempt++ {
		peerCandidates, err := collectDynamicPeerCandidates(logger, discovery, peerHealth, discoveryCache, excluded, peerLoad, peerUsage)
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
		client := p2pnet.NewClient(selected.PeerID, 10*time.Second)
		data, err := client.FetchPiece(manifest.ContentID, pieceIndex)
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

	freshCandidates, err := collectPeerCandidates(logger, options.contentID, peerAddrs, peerHealth)
	if err != nil {
		return nil, err
	}
	if discoveryCache != nil {
		discoveryCache.Store(options.contentID, freshCandidates, now)
	}
	candidates = freshCandidates
	return filterPeerCandidates(logger, options.contentID, candidates, peerHealth, excluded, now, peerLoad, peerUsage)
}

func collectPeerCandidates(logger *logging.Logger, contentID string, peerAddrs []string, peerHealth *peerHealthState) ([]scheduler.PeerCandidate, error) {
	candidates := make([]scheduler.PeerCandidate, 0, len(peerAddrs))
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
			IsLAN:      isLANAddr(addr),
			Score:      1,
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

func runServe(logger *logging.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	path := fs.String("path", "", "file to serve")
	manifestPath := fs.String("manifest", "", "optional existing manifest path")
	pieceSize := fs.Int64("piece-size", core.DefaultPieceSize, "piece size in bytes when building manifest")
	listen := fs.String("listen", "127.0.0.1:9001", "listen address")
	dataDir := fs.String("data-dir", ".p2p", "directory for generated manifest")
	lan := fs.Bool("lan", false, "enable LAN announcement")
	lanAddr := fs.String("lan-addr", p2pnet.DefaultLANDiscoveryAddr, "LAN discovery UDP target address")
	trackerURL := fs.String("tracker", "", "tracker base URL such as http://127.0.0.1:7000")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("serve requires --path")
	}

	var (
		manifest *core.ContentManifest
		err      error
	)
	if *manifestPath != "" {
		manifest, err = core.ReadManifestFile(*manifestPath)
	} else {
		manifest, err = core.BuildManifestFromFile(*path, *pieceSize)
	}
	if err != nil {
		return err
	}

	shareDir := filepath.Join(*dataDir, manifest.ContentID)
	if err := os.MkdirAll(shareDir, 0o755); err != nil {
		return err
	}
	manifestDiskPath := filepath.Join(shareDir, "manifest.json")
	if err := core.WriteManifestFile(manifestDiskPath, manifest); err != nil {
		return err
	}

	server := p2pnet.NewServer(*listen, p2pnet.StaticContentSource{
		ManifestFile: manifest,
		FilePath:     *path,
	})

	logger.Info("serve_ready",
		"contentId", manifest.ContentID,
		"listen", *listen,
		"path", *path,
		"manifestPath", manifestDiskPath,
	)

	if *lan {
		go func() {
			haveRanges := []core.HaveRange(nil)
			if len(manifest.Pieces) > 0 {
				haveRanges = []core.HaveRange{
					{Start: 0, End: len(manifest.Pieces) - 1},
				}
			}
			err := p2pnet.AnnounceLAN(context.Background(), *lanAddr, time.Second, func() p2pnet.LANAnnouncement {
				return p2pnet.BuildLANAnnouncement(
					*listen,
					*listen,
					[]p2pnet.LANContent{
						{
							ContentID:  manifest.ContentID,
							HaveRanges: haveRanges,
						},
					},
				)
			})
			if err != nil {
				logger.Error("lan_announce_failed", "lanAddr", *lanAddr, "error", err.Error())
			}
		}()
		logger.Info("lan_announce_ready", "contentId", manifest.ContentID, "listen", *listen, "lanAddr", *lanAddr)
	}

	if *trackerURL != "" {
		go func() {
			startTrackerSyncLoop(logger, *trackerURL, *listen, manifest.ContentID, time.Second, func() []core.HaveRange {
				if len(manifest.Pieces) == 0 {
					return nil
				}
				return []core.HaveRange{{Start: 0, End: len(manifest.Pieces) - 1}}
			})
		}()
		logger.Info("tracker_sync_ready", "contentId", manifest.ContentID, "tracker", *trackerURL, "peer", *listen)
	}

	fmt.Printf("contentId=%s\nmanifest=%s\nlisten=%s\n", manifest.ContentID, manifestDiskPath, *listen)
	return server.Listen(context.Background())
}

func runTracker(logger *logging.Logger, args []string) error {
	fs := flag.NewFlagSet("tracker", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:7000", "tracker listen address")

	if err := fs.Parse(args); err != nil {
		return err
	}

	server := tracker.NewServer()
	logger.Info("tracker_ready", "listen", *listen)
	return server.ListenAndServe(context.Background(), *listen)
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "path to manifest.json")
	storeDir := fs.String("store-dir", ".p2p-store", "piece store directory")
	watch := fs.Bool("watch", false, "continuously print status snapshots")
	interval := fs.Duration("interval", time.Second, "refresh interval when --watch is enabled")
	pretty := fs.Bool("pretty", false, "print a human-readable status view instead of JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("status requires --manifest")
	}

	manifest, err := core.ReadManifestFile(*manifestPath)
	if err != nil {
		return err
	}
	if *watch {
		if *interval <= 0 {
			return errors.New("status interval must be greater than zero")
		}
		return watchStatus(manifest, *storeDir, *interval, true)
	}

	return printStatusOnce(manifest, *storeDir, *pretty)
}

func printStatusOnce(manifest *core.ContentManifest, storeDir string, pretty bool) error {
	store, err := core.OpenPieceStore(filepath.Join(storeDir, manifest.ContentID), manifest)
	if err != nil {
		return err
	}
	defer store.Close()

	status := store.Status()
	if pretty {
		printPrettyStatus(status)
		return nil
	}
	out, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func watchStatus(manifest *core.ContentManifest, storeDir string, interval time.Duration, pretty bool) error {
	for {
		if err := printStatusOnce(manifest, storeDir, pretty); err != nil {
			return err
		}
		fmt.Println()
		time.Sleep(interval)
	}
}

func printPrettyStatus(status core.StoreStatus) {
	fmt.Printf(
		"content=%s state=%s progress=%.1f%% pieces=%d/%d peers=%d\n",
		shortContentID(status.ContentID),
		status.State,
		status.Progress()*100,
		status.CompletedPieces,
		status.TotalPieces,
		status.Peers,
	)
	fmt.Printf(
		"traffic down=%s up=%s downRate=%s/s upRate=%s/s\n",
		formatBytes(status.DownloadBytes),
		formatBytes(status.UploadBytes),
		formatBytes(status.DownloadRate),
		formatBytes(status.UploadRate),
	)
	fmt.Printf(
		"path lan=%s direct=%s relay=%s\n",
		formatBytes(status.PathStats.LANBytes),
		formatBytes(status.PathStats.DirectBytes),
		formatBytes(status.PathStats.RelayBytes),
	)
	if len(status.ActiveDownloads) > 0 {
		fmt.Println("activeDownloads")
		now := time.Now()
		for _, active := range status.ActiveDownloads {
			fmt.Printf(
				"  worker=%d piece=%d peer=%s age=%s started=%s\n",
				active.WorkerID,
				active.PieceIndex,
				active.PeerID,
				activeDownloadAge(active, now),
				active.StartedAt,
			)
		}
	}

	if len(status.PeerStats) == 0 {
		fmt.Println("peerStats none")
		return
	}

	type peerRow struct {
		peerID string
		stats  core.PeerStats
	}

	rows := make([]peerRow, 0, len(status.PeerStats))
	for peerID, stats := range status.PeerStats {
		rows = append(rows, peerRow{peerID: peerID, stats: stats})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].stats.DownloadedBytes != rows[j].stats.DownloadedBytes {
			return rows[i].stats.DownloadedBytes > rows[j].stats.DownloadedBytes
		}
		if rows[i].stats.UploadedBytes != rows[j].stats.UploadedBytes {
			return rows[i].stats.UploadedBytes > rows[j].stats.UploadedBytes
		}
		return rows[i].peerID < rows[j].peerID
	})

	fmt.Println("peerStats")
	for _, row := range rows {
		fmt.Printf(
			"  %s down=%s (%d pieces) up=%s (%d pieces)\n",
			row.peerID,
			formatBytes(row.stats.DownloadedBytes),
			row.stats.DownloadedPieces,
			formatBytes(row.stats.UploadedBytes),
			row.stats.UploadedPieces,
		)
	}
}

func shortContentID(contentID string) string {
	if len(contentID) <= 20 {
		return contentID
	}
	return contentID[:20] + "..."
}

func formatBytes(value int64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}

	div, exp := int64(unit), 0
	for n := value / unit; n >= unit && exp < 5; n /= unit {
		div *= unit
		exp++
	}

	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	return fmt.Sprintf("%.1f %s", float64(value)/float64(div), suffixes[exp])
}

func activeDownloadAge(active core.ActiveDownload, now time.Time) string {
	startedAt, err := time.Parse(time.RFC3339, active.StartedAt)
	if err != nil {
		return "unknown"
	}
	if now.Before(startedAt) {
		return "0s"
	}
	return formatDuration(now.Sub(startedAt))
}

func formatDuration(value time.Duration) string {
	if value < time.Second {
		return value.Truncate(time.Millisecond).String()
	}
	if value < time.Minute {
		return value.Truncate(time.Second).String()
	}
	if value < time.Hour {
		return value.Truncate(time.Second).String()
	}
	return value.Truncate(time.Minute).String()
}

func printUsage() {
	fmt.Println(`p2p commands:
  p2p share --path ./file.bin [--piece-size 1048576] [--data-dir .p2p]
  p2p tracker --listen 127.0.0.1:7000
  p2p serve --path ./file.bin [--listen 127.0.0.1:9001] [--data-dir .p2p] [--lan] [--tracker http://127.0.0.1:7000]
  p2p get --manifest .p2p/<contentId>/manifest.json --store-dir .p2p-store --out ./out.bin [--peer 127.0.0.1:9001] [--peers 127.0.0.1:9001,127.0.0.1:9002]
  p2p get --manifest .p2p/<contentId>/manifest.json --store-dir .p2p-store --out ./out.bin --listen 127.0.0.1:9002 [--seed-after-download] [--peers ...] [--lan] [--tracker http://127.0.0.1:7000]
  p2p status --manifest .p2p/<contentId>/manifest.json --store-dir .p2p-store [--pretty] [--watch] [--interval 1s]`)
}
