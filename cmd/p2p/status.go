package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"generic-p2p/internal/core"
)

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "path to manifest.json")
	storeDir := fs.String("store-dir", ".p2p-store", "piece store directory")
	watch := fs.Bool("watch", false, "continuously print status snapshots")
	interval := fs.Duration("interval", time.Second, "refresh interval when --watch is enabled")
	pretty := fs.Bool("pretty", false, "print a human-readable status view instead of JSON")
	noClear := fs.Bool("no-clear", false, "do not clear the terminal between watch refreshes")

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
		return watchStatus(manifest, *storeDir, *interval, true, !*noClear)
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

func watchStatus(manifest *core.ContentManifest, storeDir string, interval time.Duration, pretty bool, clearScreen bool) error {
	for {
		if clearScreen {
			resetTerminalView()
		}
		fmt.Printf("updated=%s interval=%s\n\n", time.Now().Format(time.RFC3339), interval)
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
	if len(status.RecentDecisions) > 0 {
		fmt.Println("recentDecisions")
		for _, decision := range status.RecentDecisions {
			if decision.TopUDPPeerID != "" {
				fmt.Printf(
					"  piece=%d selected=%s(%s score=%.2f) topUdp=%s(score=%.2f) reason=%s at=%s\n",
					decision.PieceIndex,
					decision.SelectedPeerID,
					decision.SelectedTransport,
					decision.SelectedScore,
					decision.TopUDPPeerID,
					decision.TopUDPScore,
					decision.Reason,
					decision.RecordedAt,
				)
				continue
			}
			fmt.Printf(
				"  piece=%d selected=%s(%s score=%.2f) reason=%s at=%s\n",
				decision.PieceIndex,
				decision.SelectedPeerID,
				decision.SelectedTransport,
				decision.SelectedScore,
				decision.Reason,
				decision.RecordedAt,
			)
		}
	}
	if len(status.UDPBurstProfiles) > 0 {
		fmt.Println("udpBurstProfiles")
		for _, profile := range status.UDPBurstProfiles {
			last := profile.LastSuccessAt
			result := "success"
			if profile.LastFailureAt != "" && (last == "" || profile.LastFailureAt > last) {
				last = profile.LastFailureAt
				result = "failure"
			}
			if last == "" {
				last = "-"
				result = "unknown"
			}
			fmt.Printf(
				"  %s profile=%s stage=%s last=%s at=%s failures=%d\n",
				profile.PeerID,
				profile.Profile,
				emptyDashStatus(profile.LastStage),
				result,
				last,
				profile.FailureCount,
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

func emptyDashStatus(value string) string {
	if value == "" {
		return "-"
	}
	return value
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

func resetTerminalView() {
	_, _ = os.Stdout.WriteString("\x1b[H\x1b[2J")
}
