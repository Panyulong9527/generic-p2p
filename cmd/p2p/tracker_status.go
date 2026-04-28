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
	udpMiss, udpRecovered, aligned := summarizeTrackerRouteDrift(status)
	fallbackActive := summarizeTrackerFallbackActive(status)
	publicMapped := summarizeTrackerPublicMapped(status)
	fmt.Printf(
		"tracker peers=%d swarms=%d pendingUdpProbes=%d udpProbeSuccess=%d udpProbeFailure=%d udpKeepaliveSuccess=%d udpKeepaliveFailure=%d udpBurstProfiles=%d udpDecisions=%d udpMiss=%d udpRecovered=%d routeAligned=%d udpFallbackActive=%d publicMapped=%d peerTTL=%ds cleanup=%ds\n",
		status.PeerCount,
		status.SwarmCount,
		status.PendingUDPProbeCount,
		status.RecentUDPProbeSuccesses,
		status.RecentUDPProbeFailures,
		status.RecentUDPKeepaliveSuccesses,
		status.RecentUDPKeepaliveFailures,
		len(status.UDPBurstProfiles),
		len(status.UDPDecisions),
		udpMiss,
		udpRecovered,
		aligned,
		fallbackActive,
		publicMapped,
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
	if len(status.UDPKeepaliveResults) > 0 {
		fmt.Println("udpKeepaliveResults")
		results := append([]tracker.UDPKeepaliveStatus(nil), status.UDPKeepaliveResults...)
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
				"  %s success=%d failure=%d lastSuccess=%s lastFailure=%s lastError=%s content=%s\n",
				item.TargetPeerID,
				item.SuccessCount,
				item.FailureCount,
				formatUnixTime(item.LastSuccessAt),
				formatUnixTime(item.LastFailureAt),
				emptyDash(item.LastErrorKind),
				shortContentID(item.ContentID),
			)
		}
	}
	if len(status.UDPBurstProfiles) > 0 {
		fmt.Println("udpBurstProfiles")
		items := append([]tracker.UDPBurstProfileStatus(nil), status.UDPBurstProfiles...)
		sort.Slice(items, func(i, j int) bool {
			if items[i].LastReportedAt != items[j].LastReportedAt {
				return items[i].LastReportedAt > items[j].LastReportedAt
			}
			return items[i].TargetPeerID < items[j].TargetPeerID
		})
		for _, item := range items {
			fmt.Printf(
				"  %s profile=%s lastOutcome=%s lastOutcomeAt=%s reportedAt=%s failures=%d content=%s\n",
				item.TargetPeerID,
				emptyDash(item.Profile),
				emptyDash(item.LastOutcome),
				emptyDash(item.LastOutcomeAt),
				formatUnixTime(item.LastReportedAt),
				item.FailureCount,
				shortContentID(item.ContentID),
			)
		}
	}
	if len(status.UDPDecisions) > 0 {
		fmt.Println("udpDecisions")
		items := append([]tracker.UDPDecisionStatus(nil), status.UDPDecisions...)
		sort.Slice(items, func(i, j int) bool {
			if items[i].LastReportedAt != items[j].LastReportedAt {
				return items[i].LastReportedAt > items[j].LastReportedAt
			}
			return items[i].TargetPeerID < items[j].TargetPeerID
		})
		for _, item := range items {
			fmt.Printf(
				"  %s burst=%s stage=%s budget=%d timeout=%dms score=%.2f reports=%d reason=%s content=%s reportedAt=%s\n",
				item.TargetPeerID,
				emptyDash(item.BurstProfile),
				emptyDash(item.LastStage),
				item.UDPBudget,
				item.UDPTimeoutMs,
				item.SelectedScore,
				item.ReportCount,
				emptyDash(item.Reason),
				shortContentID(item.ContentID),
				formatUnixTime(item.LastReportedAt),
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
	udpProbeResults := make(map[string]tracker.UDPProbeResultStatus, len(status.UDPProbeResults))
	for _, item := range status.UDPProbeResults {
		udpProbeResults[item.TargetPeerID] = item
	}
	peerTransferPaths := make(map[string]tracker.PeerTransferPathStatus, len(status.PeerTransferPaths))
	for _, item := range status.PeerTransferPaths {
		peerTransferPaths[item.TargetPeerID] = item
	}
	udpKeepaliveResults := make(map[string]tracker.UDPKeepaliveStatus, len(status.UDPKeepaliveResults))
	for _, item := range status.UDPKeepaliveResults {
		udpKeepaliveResults[item.TargetPeerID] = item
	}
	udpBurstProfiles := make(map[string]tracker.UDPBurstProfileStatus, len(status.UDPBurstProfiles))
	for _, item := range status.UDPBurstProfiles {
		udpBurstProfiles[item.TargetPeerID] = item
	}
	udpDecisions := make(map[string]tracker.UDPDecisionStatus, len(status.UDPDecisions))
	for _, item := range status.UDPDecisions {
		udpDecisions[item.TargetPeerID] = item
	}
	for _, swarm := range swarms {
		swarmMiss, swarmRecovered, swarmAligned := summarizeSwarmRouteDrift(swarm, udpProbeResults, peerTransferPaths)
		offenders := summarizeSwarmOffenders(swarm, udpProbeResults, peerTransferPaths)
		swarmFallbackActive := summarizeSwarmFallbackActive(swarm, udpProbeResults, peerTransferPaths, udpKeepaliveResults)
		fmt.Printf(
			"  %s peers=%d udpMiss=%d udpRecovered=%d routeAligned=%d udpFallbackActive=%d\n",
			shortContentID(swarm.ContentID),
			swarm.PeerCount,
			swarmMiss,
			swarmRecovered,
			swarmAligned,
			swarmFallbackActive,
		)
		if len(offenders) > 0 {
			fmt.Printf("    topUdpMissPeers=%s\n", strings.Join(offenders, ","))
		}

		peers := append([]tracker.PeerRecord(nil), swarm.Peers...)
		sort.Slice(peers, func(i, j int) bool {
			return peers[i].PeerID < peers[j].PeerID
		})

		for _, peer := range peers {
			advice := trackerPeerRouteAdvice(peer, udpProbeResults[peer.PeerID])
			actual := peerTransferPaths[peer.PeerID]
			fallbackState := trackerPeerFallbackState(peer, advice, actual, udpKeepaliveResults)
			decision := udpDecisions[peer.PeerID]
			fmt.Printf(
				"    %s addrs=%s observed=%s udp=%s observedUdp=%s(%s) udpHint=%s route=%s actual=%s routeDrift=%s udpFallback=%s burst=%s decision=%s have=%s lastSeen=%s\n",
				peer.PeerID,
				strings.Join(peer.Addrs, ","),
				peer.ObservedAddr,
				strings.Join(peer.UDPAddrs, ","),
				peer.ObservedUDPAddr,
				emptyDash(peer.ObservedUDPSource),
				trackerObservedUDPHint(peer),
				advice,
				emptyDash(actual.LastPath),
				trackerRouteDrift(advice, actual.LastPath),
				fallbackState,
				trackerBurstProfileSummary(udpBurstProfiles[peer.PeerID]),
				trackerUDPDecisionSummary(decision),
				formatHaveRanges(peer.HaveRanges),
				time.Unix(peer.LastSeenAt, 0).Format(time.RFC3339),
			)
		}
	}
}

func trackerObservedUDPHint(peer tracker.PeerRecord) string {
	if strings.TrimSpace(peer.ObservedUDPAddr) == "" {
		return "-"
	}
	if strings.TrimSpace(peer.ObservedUDPSource) == "stun" {
		return "public_mapped"
	}
	return "observed"
}

func trackerBurstProfileSummary(item tracker.UDPBurstProfileStatus) string {
	if strings.TrimSpace(item.Profile) == "" {
		return "-"
	}
	summary := item.Profile
	if strings.TrimSpace(item.LastOutcome) != "" {
		summary += "/" + item.LastOutcome
	}
	if item.FailureCount > 0 {
		summary += fmt.Sprintf("/f%d", item.FailureCount)
	}
	return summary
}

func trackerUDPDecisionSummary(item tracker.UDPDecisionStatus) string {
	if strings.TrimSpace(item.TargetPeerID) == "" {
		return "-"
	}
	summary := emptyDash(item.BurstProfile)
	if strings.TrimSpace(item.LastStage) != "" {
		summary += "/" + item.LastStage
	}
	if item.UDPBudget > 0 {
		summary += fmt.Sprintf("/b%d", item.UDPBudget)
	}
	if item.UDPTimeoutMs > 0 {
		summary += fmt.Sprintf("/t%dms", item.UDPTimeoutMs)
	}
	return summary
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

func summarizeTrackerRouteDrift(status tracker.StatusResponse) (int, int, int) {
	udpProbeResults := make(map[string]tracker.UDPProbeResultStatus, len(status.UDPProbeResults))
	for _, item := range status.UDPProbeResults {
		udpProbeResults[item.TargetPeerID] = item
	}
	peerTransferPaths := make(map[string]tracker.PeerTransferPathStatus, len(status.PeerTransferPaths))
	for _, item := range status.PeerTransferPaths {
		peerTransferPaths[item.TargetPeerID] = item
	}

	udpMiss := 0
	udpRecovered := 0
	aligned := 0
	for _, swarm := range status.Swarms {
		swarmMiss, swarmRecovered, swarmAligned := summarizeSwarmRouteDrift(swarm, udpProbeResults, peerTransferPaths)
		udpMiss += swarmMiss
		udpRecovered += swarmRecovered
		aligned += swarmAligned
	}
	return udpMiss, udpRecovered, aligned
}

func summarizeTrackerFallbackActive(status tracker.StatusResponse) int {
	udpProbeResults := make(map[string]tracker.UDPProbeResultStatus, len(status.UDPProbeResults))
	for _, item := range status.UDPProbeResults {
		udpProbeResults[item.TargetPeerID] = item
	}
	peerTransferPaths := make(map[string]tracker.PeerTransferPathStatus, len(status.PeerTransferPaths))
	for _, item := range status.PeerTransferPaths {
		peerTransferPaths[item.TargetPeerID] = item
	}
	udpKeepaliveResults := make(map[string]tracker.UDPKeepaliveStatus, len(status.UDPKeepaliveResults))
	for _, item := range status.UDPKeepaliveResults {
		udpKeepaliveResults[item.TargetPeerID] = item
	}
	total := 0
	for _, swarm := range status.Swarms {
		total += summarizeSwarmFallbackActive(swarm, udpProbeResults, peerTransferPaths, udpKeepaliveResults)
	}
	return total
}

func summarizeTrackerPublicMapped(status tracker.StatusResponse) int {
	total := 0
	for _, swarm := range status.Swarms {
		for _, peer := range swarm.Peers {
			if strings.TrimSpace(peer.ObservedUDPAddr) != "" && strings.TrimSpace(peer.ObservedUDPSource) == "stun" {
				total++
			}
		}
	}
	return total
}

func summarizeSwarmRouteDrift(swarm tracker.SwarmStatus, udpProbeResults map[string]tracker.UDPProbeResultStatus, peerTransferPaths map[string]tracker.PeerTransferPathStatus) (int, int, int) {
	udpMiss := 0
	udpRecovered := 0
	aligned := 0
	for _, peer := range swarm.Peers {
		actual := peerTransferPaths[peer.PeerID]
		if strings.TrimSpace(actual.LastPath) == "" {
			continue
		}
		advice := trackerPeerRouteAdvice(peer, udpProbeResults[peer.PeerID])
		switch trackerRouteDrift(advice, actual.LastPath) {
		case "udp_miss":
			udpMiss++
		case "udp_recovered":
			udpRecovered++
		default:
			aligned++
		}
	}
	return udpMiss, udpRecovered, aligned
}

func summarizeSwarmOffenders(swarm tracker.SwarmStatus, udpProbeResults map[string]tracker.UDPProbeResultStatus, peerTransferPaths map[string]tracker.PeerTransferPathStatus) []string {
	type offender struct {
		peerID   string
		tcpCount int
		lastAt   int64
	}
	offenders := make([]offender, 0, len(swarm.Peers))
	for _, peer := range swarm.Peers {
		actual := peerTransferPaths[peer.PeerID]
		if strings.TrimSpace(actual.LastPath) == "" {
			continue
		}
		advice := trackerPeerRouteAdvice(peer, udpProbeResults[peer.PeerID])
		if trackerRouteDrift(advice, actual.LastPath) != "udp_miss" {
			continue
		}
		offenders = append(offenders, offender{
			peerID:   peer.PeerID,
			tcpCount: actual.TCPCount,
			lastAt:   actual.LastAt,
		})
	}
	sort.Slice(offenders, func(i, j int) bool {
		if offenders[i].tcpCount != offenders[j].tcpCount {
			return offenders[i].tcpCount > offenders[j].tcpCount
		}
		if offenders[i].lastAt != offenders[j].lastAt {
			return offenders[i].lastAt > offenders[j].lastAt
		}
		return offenders[i].peerID < offenders[j].peerID
	})
	if len(offenders) > 3 {
		offenders = offenders[:3]
	}
	result := make([]string, 0, len(offenders))
	for _, item := range offenders {
		result = append(result, fmt.Sprintf("%s(tcp=%d)", item.peerID, item.tcpCount))
	}
	return result
}

func summarizeSwarmFallbackActive(swarm tracker.SwarmStatus, udpProbeResults map[string]tracker.UDPProbeResultStatus, peerTransferPaths map[string]tracker.PeerTransferPathStatus, udpKeepaliveResults map[string]tracker.UDPKeepaliveStatus) int {
	total := 0
	for _, peer := range swarm.Peers {
		advice := trackerPeerRouteAdvice(peer, udpProbeResults[peer.PeerID])
		if trackerPeerFallbackState(peer, advice, peerTransferPaths[peer.PeerID], udpKeepaliveResults) == "active" {
			total++
		}
	}
	return total
}

func trackerPeerFallbackState(peer tracker.PeerRecord, advice string, actual tracker.PeerTransferPathStatus, udpKeepaliveResults map[string]tracker.UDPKeepaliveStatus) string {
	if trackerRouteDrift(advice, actual.LastPath) != "udp_miss" {
		return "-"
	}
	latestKeepalive := tracker.UDPKeepaliveStatus{}
	for _, key := range trackerPeerKeepaliveKeys(peer) {
		result, ok := udpKeepaliveResults[key]
		if !ok {
			continue
		}
		if maxInt64(result.LastSuccessAt, result.LastFailureAt) > maxInt64(latestKeepalive.LastSuccessAt, latestKeepalive.LastFailureAt) {
			latestKeepalive = result
		}
	}
	if latestKeepalive.TargetPeerID == "" {
		return "pending"
	}
	if latestKeepalive.LastFailureAt > latestKeepalive.LastSuccessAt {
		return "active"
	}
	return "cooling"
}

func trackerPeerKeepaliveKeys(peer tracker.PeerRecord) []string {
	keys := make([]string, 0, len(peer.UDPAddrs)+1)
	if strings.TrimSpace(peer.ObservedUDPAddr) != "" {
		keys = append(keys, "udp://"+peer.ObservedUDPAddr)
	}
	for _, addr := range peer.UDPAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		key := "udp://" + addr
		exists := false
		for _, current := range keys {
			if current == key {
				exists = true
				break
			}
		}
		if !exists {
			keys = append(keys, key)
		}
	}
	return keys
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
