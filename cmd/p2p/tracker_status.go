package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"generic-p2p/internal/core"
	"generic-p2p/internal/tracker"
)

func runTrackerStatus(args []string) error {
	fs := flag.NewFlagSet("tracker-status", flag.ContinueOnError)
	trackerURL := fs.String("tracker", "", "tracker base URL such as http://127.0.0.1:7000")
	watch := fs.Bool("watch", false, "continuously print tracker status snapshots")
	interval := fs.Duration("interval", time.Second, "refresh interval when --watch is enabled")
	pretty := fs.Bool("pretty", false, "print a human-readable tracker view instead of JSON")
	noClear := fs.Bool("no-clear", false, "do not clear the terminal between watch refreshes")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*trackerURL) == "" {
		return errors.New("tracker-status requires --tracker")
	}

	if *watch {
		if *interval <= 0 {
			return errors.New("tracker-status interval must be greater than zero")
		}
		return watchTrackerStatus(*trackerURL, *interval, true, !*noClear)
	}

	return printTrackerStatusOnce(*trackerURL, *pretty)
}

func printTrackerStatusOnce(trackerURL string, pretty bool) error {
	client := tracker.NewClient(trackerURL)
	status, err := client.GetStatus(context.Background())
	if err != nil {
		return err
	}

	if pretty {
		printPrettyTrackerStatus(status)
		return nil
	}

	out, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func watchTrackerStatus(trackerURL string, interval time.Duration, pretty bool, clearScreen bool) error {
	for {
		if clearScreen {
			resetTerminalView()
		}
		fmt.Printf("updated=%s interval=%s\n\n", time.Now().Format(time.RFC3339), interval)
		if err := printTrackerStatusOnce(trackerURL, pretty); err != nil {
			return err
		}
		fmt.Println()
		time.Sleep(interval)
	}
}

func printPrettyTrackerStatus(status tracker.StatusResponse) {
	fmt.Printf(
		"tracker peers=%d swarms=%d pendingUdpProbes=%d udpProbeSuccess=%d udpProbeFailure=%d peerTTL=%ds cleanup=%ds\n",
		status.PeerCount,
		status.SwarmCount,
		status.PendingUDPProbeCount,
		status.RecentUDPProbeSuccesses,
		status.RecentUDPProbeFailures,
		status.PeerTTLSeconds,
		status.CleanupIntervalSeconds,
	)
	if status.StatePath != "" {
		fmt.Printf("stateFile=%s\n", status.StatePath)
	}
	if len(status.PendingUDPProbes) > 0 {
		fmt.Println("pendingUdpProbes")
		pending := append([]tracker.PendingUDPProbeStatus(nil), status.PendingUDPProbes...)
		sort.Slice(pending, func(i, j int) bool {
			if pending[i].RequestCount != pending[j].RequestCount {
				return pending[i].RequestCount > pending[j].RequestCount
			}
			return pending[i].TargetPeerID < pending[j].TargetPeerID
		})
		for _, item := range pending {
			fmt.Printf("  %s requests=%d\n", item.TargetPeerID, item.RequestCount)
		}
	}
	if len(status.UDPProbeResults) > 0 {
		fmt.Println("udpProbeResults")
		results := append([]tracker.UDPProbeResultStatus(nil), status.UDPProbeResults...)
		sort.Slice(results, func(i, j int) bool {
			left := maxInt64(results[i].LastFailureAt, results[i].LastSuccessAt)
			right := maxInt64(results[j].LastFailureAt, results[j].LastSuccessAt)
			if left != right {
				return left > right
			}
			return results[i].TargetPeerID < results[j].TargetPeerID
		})
		for _, item := range results {
			fmt.Printf(
				"  %s success=%d failure=%d lastSuccess=%s lastFailure=%s lastError=%s requester=%s content=%s\n",
				item.TargetPeerID,
				item.SuccessCount,
				item.FailureCount,
				formatUnixTime(item.LastSuccessAt),
				formatUnixTime(item.LastFailureAt),
				emptyDash(item.LastErrorKind),
				emptyDash(item.LastRequesterID),
				shortContentID(item.LastContentID),
			)
		}
	}
	if len(status.PeerTransferPaths) > 0 {
		fmt.Println("peerTransferPaths")
		paths := append([]tracker.PeerTransferPathStatus(nil), status.PeerTransferPaths...)
		sort.Slice(paths, func(i, j int) bool {
			if paths[i].LastAt != paths[j].LastAt {
				return paths[i].LastAt > paths[j].LastAt
			}
			return paths[i].TargetPeerID < paths[j].TargetPeerID
		})
		for _, item := range paths {
			fmt.Printf(
				"  %s lastPath=%s lastAt=%s udp=%d tcp=%d content=%s\n",
				item.TargetPeerID,
				emptyDash(item.LastPath),
				formatUnixTime(item.LastAt),
				item.UDPCount,
				item.TCPCount,
				shortContentID(item.ContentID),
			)
		}
	}
	if len(status.Swarms) == 0 {
		fmt.Println("swarms none")
		return
	}

	swarms := append([]tracker.SwarmStatus(nil), status.Swarms...)
	sort.Slice(swarms, func(i, j int) bool {
		if swarms[i].PeerCount != swarms[j].PeerCount {
			return swarms[i].PeerCount > swarms[j].PeerCount
		}
		return swarms[i].ContentID < swarms[j].ContentID
	})

	fmt.Println("swarms")
	for _, swarm := range swarms {
		fmt.Printf("  %s peers=%d\n", shortContentID(swarm.ContentID), swarm.PeerCount)

		peers := append([]tracker.PeerRecord(nil), swarm.Peers...)
		udpProbeResults := make(map[string]tracker.UDPProbeResultStatus, len(status.UDPProbeResults))
		for _, item := range status.UDPProbeResults {
			udpProbeResults[item.TargetPeerID] = item
		}
		peerTransferPaths := make(map[string]tracker.PeerTransferPathStatus, len(status.PeerTransferPaths))
		for _, item := range status.PeerTransferPaths {
			peerTransferPaths[item.TargetPeerID] = item
		}
		sort.Slice(peers, func(i, j int) bool {
			return peers[i].PeerID < peers[j].PeerID
		})

		for _, peer := range peers {
			advice := trackerPeerRouteAdvice(peer, udpProbeResults[peer.PeerID])
			actual := peerTransferPaths[peer.PeerID]
			fmt.Printf(
				"    %s addrs=%s observed=%s udp=%s observedUdp=%s route=%s actual=%s routeDrift=%s have=%s lastSeen=%s\n",
				peer.PeerID,
				strings.Join(peer.Addrs, ","),
				peer.ObservedAddr,
				strings.Join(peer.UDPAddrs, ","),
				peer.ObservedUDPAddr,
				advice,
				emptyDash(actual.LastPath),
				trackerRouteDrift(advice, actual.LastPath),
				formatHaveRanges(peer.HaveRanges),
				time.Unix(peer.LastSeenAt, 0).Format(time.RFC3339),
			)
		}
	}
}

func trackerPeerRouteAdvice(peer tracker.PeerRecord, result tracker.UDPProbeResultStatus) string {
	hasUDPPath := len(peer.UDPAddrs) > 0 || strings.TrimSpace(peer.ObservedUDPAddr) != ""
	if !hasUDPPath {
		return "tcp_only"
	}
	if result.TargetPeerID == "" {
		if strings.TrimSpace(peer.ObservedUDPAddr) != "" {
			return "prefer_udp"
		}
		return "try_udp"
	}
	if result.LastSuccessAt > result.LastFailureAt {
		return "prefer_udp"
	}
	if result.LastFailureAt > 0 {
		if strings.TrimSpace(result.LastErrorKind) == "udp_timeout" {
			return "udp_fallback"
		}
		return "prefer_tcp"
	}
	if strings.TrimSpace(peer.ObservedUDPAddr) != "" {
		return "prefer_udp"
	}
	return "try_udp"
}

func trackerRouteDrift(advice string, actual string) string {
	recommended := ""
	switch advice {
	case "prefer_udp", "try_udp", "udp_fallback":
		recommended = "udp"
	case "prefer_tcp", "tcp_only":
		recommended = "tcp"
	}
	actual = strings.TrimSpace(strings.ToLower(actual))
	if recommended == "" || actual == "" {
		return "-"
	}
	if recommended == "udp" && actual == "tcp" {
		return "udp_miss"
	}
	if recommended == "tcp" && actual == "udp" {
		return "udp_recovered"
	}
	return "aligned"
}

func formatHaveRanges(ranges []core.HaveRange) string {
	if len(ranges) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(ranges))
	for _, current := range ranges {
		if current.Start == current.End {
			parts = append(parts, fmt.Sprintf("%d", current.Start))
			continue
		}
		parts = append(parts, fmt.Sprintf("%d-%d", current.Start, current.End))
	}
	return strings.Join(parts, ",")
}

func formatUnixTime(value int64) string {
	if value == 0 {
		return "-"
	}
	return time.Unix(value, 0).Format(time.RFC3339)
}

func emptyDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func maxInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
