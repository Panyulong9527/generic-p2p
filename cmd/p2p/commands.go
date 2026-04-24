package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/logging"
	p2pnet "generic-p2p/internal/net"
	"generic-p2p/internal/tracker"
)

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
	stateFile := fs.String("state-file", ".p2p-tracker-state.json", "tracker state file path")
	webDataDir := fs.String("web-data-dir", ".p2p-web", "directory for demo web uploads and share index")
	peerTTL := fs.Duration("peer-ttl", 10*time.Second, "how long a peer remains active without refresh")
	cleanupInterval := fs.Duration("cleanup-interval", 2*time.Second, "how often expired peers are pruned; use 0 to disable background cleanup")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerTTL <= 0 {
		return errors.New("tracker peer TTL must be greater than zero")
	}
	if *cleanupInterval < 0 {
		return errors.New("tracker cleanup interval cannot be negative")
	}

	server := tracker.NewServer().
		WithStatePath(*stateFile).
		WithWebDataDir(*webDataDir).
		WithPeerTTL(*peerTTL).
		WithCleanupInterval(*cleanupInterval)
	logger.Info(
		"tracker_ready",
		"listen", *listen,
		"stateFile", *stateFile,
		"webDataDir", *webDataDir,
		"peerTTL", peerTTL.String(),
		"cleanupInterval", cleanupInterval.String(),
	)
	return server.ListenAndServe(context.Background(), *listen)
}
