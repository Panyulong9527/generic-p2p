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
		"tracker peers=%d swarms=%d peerTTL=%ds cleanup=%ds\n",
		status.PeerCount,
		status.SwarmCount,
		status.PeerTTLSeconds,
		status.CleanupIntervalSeconds,
	)
	if status.StatePath != "" {
		fmt.Printf("stateFile=%s\n", status.StatePath)
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
		sort.Slice(peers, func(i, j int) bool {
			return peers[i].PeerID < peers[j].PeerID
		})

		for _, peer := range peers {
			fmt.Printf(
				"    %s addrs=%s observed=%s udp=%s observedUdp=%s have=%s lastSeen=%s\n",
				peer.PeerID,
				strings.Join(peer.Addrs, ","),
				peer.ObservedAddr,
				strings.Join(peer.UDPAddrs, ","),
				peer.ObservedUDPAddr,
				formatHaveRanges(peer.HaveRanges),
				time.Unix(peer.LastSeenAt, 0).Format(time.RFC3339),
			)
		}
	}
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
