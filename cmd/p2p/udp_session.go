package main

import (
	"sort"
	"strings"
	"sync"
	"time"

	p2pnet "generic-p2p/internal/net"
)

type udpPeerSession struct {
	PeerID                   string
	PrimaryAddr              string
	Addresses                map[string]time.Time
	State                    string
	PreferredRole            string
	StickyContentID          string
	LeaseContentID           string
	LeaseUntil               time.Time
	ContentPieceRuns         int
	HandoffContentID         string
	HandoffUntil             time.Time
	OwnerContentID           string
	OwnerWorkerID            int
	OwnerUntil               time.Time
	ConsecutivePieceRuns     int
	HealthScore              float64
	LastStage                string
	LastErrorKind            string
	SuccessStreak            int
	FailureStreak            int
	RecommendedChunkWindow   int
	RecommendedRoundTimeout  time.Duration
	RecommendedAttemptBudget int
	KeepaliveInterval        time.Duration
	ChunkWindowDelta         int
	RoundTimeoutDelta        time.Duration
	AttemptBudgetDelta       int
	PipelineDepth            int
	PipelineUntil            time.Time
	LastActiveAt             time.Time
	LastSuccessAt            time.Time
	LastFailureAt            time.Time
	LastKeepaliveAt          time.Time
	LastObservedAt           time.Time
	StickyUntil              time.Time
	QuarantineUntil          time.Time
	ObservedSource           string
	RecentContentIDs         map[string]time.Time
}

var udpSessionState = struct {
	mu       sync.Mutex
	sessions map[string]udpPeerSession
}{
	sessions: make(map[string]udpPeerSession),
}

type udpContentRouteLease struct {
	ContentID     string
	OwnerPeerID   string
	HandoffPeerID string
	TakeoverUntil time.Time
	HandoffUntil  time.Time
	InflightCount int
	InflightLimit int
	LastUpdatedAt time.Time
}

var udpContentRouteState = struct {
	mu     sync.Mutex
	leases map[string]udpContentRouteLease
}{
	leases: make(map[string]udpContentRouteLease),
}

func noteUDPSessionAddr(peerID string, remoteAddr string, source string, contentID string, now time.Time) {
	peerID = strings.TrimSpace(peerID)
	remoteAddr = strings.TrimSpace(remoteAddr)
	if peerID == "" || remoteAddr == "" {
		return
	}

	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	session := udpSessionState.sessions[peerID]
	session.PeerID = peerID
	if session.Addresses == nil {
		session.Addresses = make(map[string]time.Time)
	}
	if session.RecentContentIDs == nil {
		session.RecentContentIDs = make(map[string]time.Time)
	}
	session.Addresses[remoteAddr] = now
	session.PrimaryAddr = remoteAddr
	session.LastObservedAt = now
	session.LastActiveAt = maxSessionTime(session.LastActiveAt, now)
	session.State = udpSessionStateKind(session, now)
	refreshUDPSessionRecommendations(&session, now)
	if strings.TrimSpace(source) != "" {
		session.ObservedSource = strings.TrimSpace(source)
	}
	if strings.TrimSpace(contentID) != "" {
		session.RecentContentIDs[strings.TrimSpace(contentID)] = now
	}
	udpSessionState.sessions[peerID] = session
}

func noteUDPSessionSuccess(peerID string, remoteAddr string, contentID string, now time.Time) {
	noteUDPSessionEvent(peerID, remoteAddr, "unknown", true, "", contentID, now)
}

func noteUDPSessionFailure(peerID string, remoteAddr string, now time.Time) {
	noteUDPSessionEvent(peerID, remoteAddr, "unknown", false, "", "", now)
}

func noteUDPSessionStageSuccess(peerID string, remoteAddr string, stage string, contentID string, now time.Time) {
	noteUDPSessionEvent(peerID, remoteAddr, stage, true, "", contentID, now)
}

func noteUDPSessionStageFailure(peerID string, remoteAddr string, stage string, errorKind string, now time.Time) {
	noteUDPSessionEvent(peerID, remoteAddr, stage, false, errorKind, "", now)
}

func noteUDPSessionEvent(peerID string, remoteAddr string, stage string, success bool, errorKind string, contentID string, now time.Time) {
	peerID = strings.TrimSpace(peerID)
	remoteAddr = strings.TrimSpace(remoteAddr)
	if peerID == "" {
		return
	}

	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	session := udpSessionState.sessions[peerID]
	session.PeerID = peerID
	if session.Addresses == nil {
		session.Addresses = make(map[string]time.Time)
	}
	if session.RecentContentIDs == nil {
		session.RecentContentIDs = make(map[string]time.Time)
	}
	if remoteAddr != "" {
		session.Addresses[remoteAddr] = now
		session.PrimaryAddr = remoteAddr
	}
	session.LastStage = strings.TrimSpace(stage)
	if success {
		session.LastSuccessAt = now
		session.SuccessStreak++
		session.FailureStreak = 0
		session.LastErrorKind = ""
		session.HealthScore = clampUDPSessionHealth(session.HealthScore + udpSessionStageDelta(stage, true))
	} else {
		session.LastFailureAt = now
		session.FailureStreak++
		session.SuccessStreak = 0
		session.LastErrorKind = strings.TrimSpace(errorKind)
		session.HealthScore = clampUDPSessionHealth(session.HealthScore + udpSessionStageDelta(stage, false))
	}
	updateUDPSessionPathAffinity(&session, stage, success, contentID, now)
	session.LastActiveAt = now
	session.State = udpSessionStateKind(session, now)
	refreshUDPSessionRecommendations(&session, now)
	if strings.TrimSpace(contentID) != "" {
		session.RecentContentIDs[strings.TrimSpace(contentID)] = now
	}
	udpSessionState.sessions[peerID] = session
}

func noteUDPSessionKeepalive(peerID string, remoteAddr string, now time.Time) {
	peerID = strings.TrimSpace(peerID)
	if peerID == "" {
		return
	}

	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	session := udpSessionState.sessions[peerID]
	session.PeerID = peerID
	if session.Addresses == nil {
		session.Addresses = make(map[string]time.Time)
	}
	if remoteAddr = strings.TrimSpace(remoteAddr); remoteAddr != "" {
		session.Addresses[remoteAddr] = now
		if session.PrimaryAddr == "" {
			session.PrimaryAddr = remoteAddr
		}
	}
	session.LastKeepaliveAt = now
	session.LastActiveAt = maxSessionTime(session.LastActiveAt, now)
	session.State = udpSessionStateKind(session, now)
	refreshUDPSessionRecommendations(&session, now)
	udpSessionState.sessions[peerID] = session
}

func noteUDPSessionPieceRound(peerID string, remoteAddr string, stats p2pnet.UDPPieceRoundStats, now time.Time) {
	peerID = strings.TrimSpace(peerID)
	remoteAddr = strings.TrimSpace(remoteAddr)
	if peerID == "" || stats.RequestedChunks <= 0 {
		return
	}

	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	session := udpSessionState.sessions[peerID]
	session.PeerID = peerID
	if session.Addresses == nil {
		session.Addresses = make(map[string]time.Time)
	}
	if remoteAddr != "" {
		session.Addresses[remoteAddr] = now
		session.PrimaryAddr = remoteAddr
	}
	receiveRatio := float64(stats.ReceivedChunks) / float64(stats.RequestedChunks)
	switch {
	case stats.Completed && receiveRatio >= 0.9 && stats.Duration <= 800*time.Millisecond:
		if session.ChunkWindowDelta < 1 {
			session.ChunkWindowDelta++
		}
		if session.RoundTimeoutDelta > -200*time.Millisecond {
			session.RoundTimeoutDelta -= 100 * time.Millisecond
		}
		if session.AttemptBudgetDelta < 1 {
			session.AttemptBudgetDelta++
		}
		if session.PipelineDepth < 2 {
			session.PipelineDepth++
		}
		session.PipelineUntil = now.Add(18 * time.Second)
	case stats.RequestedChunks > 0 && stats.ReceivedChunks == 0:
		if session.ChunkWindowDelta > -1 {
			session.ChunkWindowDelta--
		}
		if session.RoundTimeoutDelta < 250*time.Millisecond {
			session.RoundTimeoutDelta += 150 * time.Millisecond
		}
		if session.AttemptBudgetDelta > -1 {
			session.AttemptBudgetDelta--
		}
		if session.PipelineDepth > 0 {
			session.PipelineDepth--
		}
		if session.PipelineDepth == 0 {
			session.PipelineUntil = time.Time{}
		}
	case receiveRatio < 0.5:
		if session.ChunkWindowDelta > -1 {
			session.ChunkWindowDelta--
		}
		if session.RoundTimeoutDelta < 200*time.Millisecond {
			session.RoundTimeoutDelta += 100 * time.Millisecond
		}
		if session.PipelineDepth > 0 {
			session.PipelineDepth--
		}
		if session.PipelineDepth == 0 {
			session.PipelineUntil = time.Time{}
		}
	case receiveRatio >= 0.85 && stats.Duration <= 900*time.Millisecond:
		if session.ChunkWindowDelta < 1 {
			session.ChunkWindowDelta++
		}
		if session.RoundTimeoutDelta > -150*time.Millisecond {
			session.RoundTimeoutDelta -= 50 * time.Millisecond
		}
		if session.PipelineDepth < 1 {
			session.PipelineDepth = 1
		}
		if session.PipelineUntil.Before(now.Add(12 * time.Second)) {
			session.PipelineUntil = now.Add(12 * time.Second)
		}
	}
	session.LastActiveAt = now
	refreshUDPSessionRecommendations(&session, now)
	udpSessionState.sessions[peerID] = session
}

func udpSessionPreferredAddr(peerID string, now time.Time) (string, bool) {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return "", false
	}
	if strings.TrimSpace(session.PrimaryAddr) == "" {
		return "", false
	}
	return session.PrimaryAddr, true
}

func udpSessionAddrs(peerID string, now time.Time) []string {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return nil
	}
	return udpSessionOrderedAddrs(session)
}

func udpWarmSessionPeers(contentID string, now time.Time) []udpPeerSession {
	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	sessions := make([]udpPeerSession, 0, len(udpSessionState.sessions))
	for peerID, session := range udpSessionState.sessions {
		session = pruneUDPSession(session, now)
		if sessionExpired(session, contentID, now) {
			delete(udpSessionState.sessions, peerID)
			continue
		}
		if !sessionShouldKeepAlive(session, contentID, now) {
			udpSessionState.sessions[peerID] = session
			continue
		}
		sessions = append(sessions, cloneUDPSession(session))
		udpSessionState.sessions[peerID] = session
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].LastSuccessAt.After(sessions[j].LastSuccessAt)
	})
	return sessions
}

func currentUDPSessionSnapshots(contentID string, now time.Time) []udpPeerSession {
	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	sessions := make([]udpPeerSession, 0, len(udpSessionState.sessions))
	for peerID, session := range udpSessionState.sessions {
		session = pruneUDPSession(session, now)
		if sessionExpired(session, contentID, now) {
			delete(udpSessionState.sessions, peerID)
			continue
		}
		if !sessionRelevantToContent(session, contentID, now) {
			udpSessionState.sessions[peerID] = session
			continue
		}
		sessions = append(sessions, cloneUDPSession(session))
		udpSessionState.sessions[peerID] = session
	}
	sort.Slice(sessions, func(i, j int) bool {
		left := maxSessionTime(sessions[i].LastSuccessAt, sessions[i].LastActiveAt)
		right := maxSessionTime(sessions[j].LastSuccessAt, sessions[j].LastActiveAt)
		return left.After(right)
	})
	return sessions
}

func udpSessionShouldSendKeepalive(peerID string, remoteAddr string, now time.Time) bool {
	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	session, ok := udpSessionState.sessions[strings.TrimSpace(peerID)]
	if !ok {
		return false
	}
	session = pruneUDPSession(session, now)
	if sessionExpired(session, "", now) {
		delete(udpSessionState.sessions, strings.TrimSpace(peerID))
		return false
	}
	if strings.TrimSpace(remoteAddr) == "" || !sessionHasAddr(session, remoteAddr) {
		udpSessionState.sessions[strings.TrimSpace(peerID)] = session
		return false
	}
	if now.Sub(session.LastKeepaliveAt) < udpSessionKeepaliveInterval(session, now) {
		udpSessionState.sessions[strings.TrimSpace(peerID)] = session
		return false
	}
	session.LastKeepaliveAt = now
	session.LastActiveAt = maxSessionTime(session.LastActiveAt, now)
	session.State = udpSessionStateKind(session, now)
	refreshUDPSessionRecommendations(&session, now)
	udpSessionState.sessions[strings.TrimSpace(peerID)] = session
	return true
}

func udpSessionDiscoveryBias(contentID string, peerID string, now time.Time) float64 {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	switch udpSessionStateKind(session, now) {
	case "active":
		if session.HealthScore >= 0.45 {
			return 0.12
		}
		return 0.10
	case "warm":
		return 0.05
	case "cooling":
		return -0.08
	}
	if contentID != "" {
		if seenAt, ok := session.RecentContentIDs[contentID]; ok && now.Sub(seenAt) <= 90*time.Second {
			if !session.LastSuccessAt.IsZero() && now.Sub(session.LastSuccessAt) <= 20*time.Second {
				return 0.08
			}
			if !session.LastSuccessAt.IsZero() && now.Sub(session.LastSuccessAt) <= 60*time.Second {
				return 0.04
			}
		}
	}
	return 0
}

func udpSessionSnapshot(peerID string, now time.Time) (udpPeerSession, bool) {
	udpSessionState.mu.Lock()
	defer udpSessionState.mu.Unlock()

	session, ok := udpSessionState.sessions[strings.TrimSpace(peerID)]
	if !ok {
		return udpPeerSession{}, false
	}
	session = pruneUDPSession(session, now)
	if sessionExpired(session, "", now) {
		delete(udpSessionState.sessions, strings.TrimSpace(peerID))
		return udpPeerSession{}, false
	}
	udpSessionState.sessions[strings.TrimSpace(peerID)] = session
	return cloneUDPSession(session), true
}

func udpSessionOrderedAddrs(session udpPeerSession) []string {
	addrs := make([]string, 0, len(session.Addresses))
	if strings.TrimSpace(session.PrimaryAddr) != "" {
		addrs = append(addrs, session.PrimaryAddr)
	}
	type candidateAddr struct {
		addr   string
		seenAt time.Time
	}
	rest := make([]candidateAddr, 0, len(session.Addresses))
	for addr, seenAt := range session.Addresses {
		if strings.TrimSpace(addr) == "" || addr == session.PrimaryAddr {
			continue
		}
		rest = append(rest, candidateAddr{addr: addr, seenAt: seenAt})
	}
	sort.Slice(rest, func(i, j int) bool {
		return rest[i].seenAt.After(rest[j].seenAt)
	})
	for _, item := range rest {
		addrs = append(addrs, item.addr)
	}
	return addrs
}

func pruneUDPSession(session udpPeerSession, now time.Time) udpPeerSession {
	for addr, seenAt := range session.Addresses {
		if now.Sub(seenAt) > 10*time.Minute {
			delete(session.Addresses, addr)
		}
	}
	for contentID, seenAt := range session.RecentContentIDs {
		if now.Sub(seenAt) > 10*time.Minute {
			delete(session.RecentContentIDs, contentID)
		}
	}
	if !sessionHasAddr(session, session.PrimaryAddr) {
		session.PrimaryAddr = ""
	}
	if session.PrimaryAddr == "" {
		for _, addr := range udpSessionOrderedAddrs(session) {
			session.PrimaryAddr = addr
			break
		}
	}
	session.State = udpSessionStateKind(session, now)
	refreshUDPSessionRecommendations(&session, now)
	return session
}

func sessionExpired(session udpPeerSession, contentID string, now time.Time) bool {
	if strings.TrimSpace(session.PrimaryAddr) == "" && len(session.Addresses) == 0 {
		return true
	}
	if !session.LastSuccessAt.IsZero() && now.Sub(session.LastSuccessAt) <= 2*time.Minute {
		return false
	}
	if contentID != "" {
		if seenAt, ok := session.RecentContentIDs[contentID]; ok && now.Sub(seenAt) <= 2*time.Minute {
			return false
		}
	}
	return now.Sub(session.LastActiveAt) > 2*time.Minute
}

func sessionShouldKeepAlive(session udpPeerSession, contentID string, now time.Time) bool {
	if strings.TrimSpace(session.PrimaryAddr) == "" {
		return false
	}
	if !session.LastSuccessAt.IsZero() && now.Sub(session.LastSuccessAt) <= 90*time.Second {
		return true
	}
	if contentID != "" {
		if seenAt, ok := session.RecentContentIDs[contentID]; ok && now.Sub(seenAt) <= 90*time.Second {
			return true
		}
	}
	return false
}

func sessionRelevantToContent(session udpPeerSession, contentID string, now time.Time) bool {
	if strings.TrimSpace(contentID) == "" {
		return now.Sub(session.LastActiveAt) <= 2*time.Minute || now.Sub(session.LastSuccessAt) <= 2*time.Minute
	}
	if seenAt, ok := session.RecentContentIDs[strings.TrimSpace(contentID)]; ok && now.Sub(seenAt) <= 2*time.Minute {
		return true
	}
	return now.Sub(session.LastSuccessAt) <= 45*time.Second
}

func udpSessionKeepaliveInterval(session udpPeerSession, now time.Time) time.Duration {
	if session.KeepaliveInterval > 0 {
		return session.KeepaliveInterval
	}
	switch udpSessionStateKind(session, now) {
	case "active":
		return 6 * time.Second
	case "warm":
		return 10 * time.Second
	case "cooling":
		return 15 * time.Second
	}
	return 15 * time.Second
}

func udpSessionStateForPeer(peerID string, now time.Time) string {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return ""
	}
	return udpSessionStateKind(session, now)
}

func udpSessionStickyRole(peerID string, contentID string, now time.Time) string {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return ""
	}
	if session.StickyUntil.IsZero() || now.After(session.StickyUntil) {
		return ""
	}
	if strings.TrimSpace(contentID) != "" && strings.TrimSpace(session.StickyContentID) != "" && strings.TrimSpace(session.StickyContentID) != strings.TrimSpace(contentID) {
		return ""
	}
	return strings.TrimSpace(session.PreferredRole)
}

func udpSessionIsQuarantined(peerID string, now time.Time) bool {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return false
	}
	return !session.QuarantineUntil.IsZero() && now.Before(session.QuarantineUntil)
}

func noteUDPSessionPieceOwnership(peerID string, remoteAddr string, contentID string, workerID int, success bool, now time.Time) {
	peerID = strings.TrimSpace(peerID)
	remoteAddr = strings.TrimSpace(remoteAddr)
	contentID = strings.TrimSpace(contentID)
	if peerID == "" || contentID == "" {
		return
	}

	shouldMarkRouteSuccess := false
	shouldMarkRouteHandoff := false

	udpSessionState.mu.Lock()

	session := udpSessionState.sessions[peerID]
	session.PeerID = peerID
	if session.Addresses == nil {
		session.Addresses = make(map[string]time.Time)
	}
	if session.RecentContentIDs == nil {
		session.RecentContentIDs = make(map[string]time.Time)
	}
	if remoteAddr != "" {
		session.Addresses[remoteAddr] = now
		session.PrimaryAddr = remoteAddr
	}
	if success {
		session.HandoffContentID = ""
		session.HandoffUntil = time.Time{}
		shouldMarkRouteSuccess = true
		if session.LeaseContentID == contentID && !session.LeaseUntil.IsZero() && now.Before(session.LeaseUntil) {
			session.ContentPieceRuns++
		} else {
			session.ContentPieceRuns = 1
		}
		session.LeaseContentID = contentID
		session.LeaseUntil = now.Add(25 * time.Second)
		if session.OwnerContentID == contentID && session.OwnerWorkerID == workerID && !session.OwnerUntil.IsZero() && now.Before(session.OwnerUntil) {
			session.ConsecutivePieceRuns++
		} else {
			session.ConsecutivePieceRuns = 1
		}
		session.OwnerContentID = contentID
		session.OwnerWorkerID = workerID
		session.OwnerUntil = now.Add(20 * time.Second)
	} else {
		if session.LeaseContentID == contentID {
			if session.ContentPieceRuns > 0 {
				session.ContentPieceRuns--
			}
			if session.ContentPieceRuns == 0 {
				session.LeaseUntil = now.Add(4 * time.Second)
			} else {
				session.LeaseUntil = now.Add(8 * time.Second)
			}
			if session.ContentPieceRuns <= 1 {
				session.HandoffContentID = contentID
				session.HandoffUntil = now.Add(14 * time.Second)
				shouldMarkRouteHandoff = true
			}
		}
		if session.OwnerContentID == contentID {
			session.ConsecutivePieceRuns = 0
			session.OwnerUntil = now.Add(4 * time.Second)
		}
	}
	session.LastActiveAt = now
	refreshUDPSessionRecommendations(&session, now)
	udpSessionState.sessions[peerID] = session
	udpSessionState.mu.Unlock()

	if shouldMarkRouteSuccess {
		noteUDPContentRouteSuccess(contentID, peerID, now)
	}
	if shouldMarkRouteHandoff {
		noteUDPContentRouteHandoff(contentID, peerID, now)
	}
}

func udpSessionContentRun(peerID string, contentID string, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	if session.LeaseUntil.IsZero() || now.After(session.LeaseUntil) {
		return 0
	}
	if strings.TrimSpace(session.LeaseContentID) != strings.TrimSpace(contentID) {
		return 0
	}
	return session.ContentPieceRuns
}

func udpSessionInHandoff(peerID string, contentID string, now time.Time) bool {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return false
	}
	if session.HandoffUntil.IsZero() || now.After(session.HandoffUntil) {
		return false
	}
	if strings.TrimSpace(session.HandoffContentID) != strings.TrimSpace(contentID) {
		return false
	}
	return true
}

func noteUDPContentRouteSuccess(contentID string, peerID string, now time.Time) {
	contentID = strings.TrimSpace(contentID)
	peerID = strings.TrimSpace(peerID)
	if contentID == "" || peerID == "" {
		return
	}
	udpContentRouteState.mu.Lock()
	defer udpContentRouteState.mu.Unlock()
	lease := udpContentRouteState.leases[contentID]
	lease.ContentID = contentID
	lease.OwnerPeerID = peerID
	lease.HandoffPeerID = ""
	lease.TakeoverUntil = now.Add(18 * time.Second)
	lease.HandoffUntil = time.Time{}
	lease.InflightLimit = suggestedUDPContentRouteInflightLimit(contentID, peerID, now)
	lease.LastUpdatedAt = now
	udpContentRouteState.leases[contentID] = lease
}

func noteUDPContentRouteHandoff(contentID string, peerID string, now time.Time) {
	contentID = strings.TrimSpace(contentID)
	peerID = strings.TrimSpace(peerID)
	if contentID == "" || peerID == "" {
		return
	}
	udpContentRouteState.mu.Lock()
	defer udpContentRouteState.mu.Unlock()
	lease := udpContentRouteState.leases[contentID]
	lease.ContentID = contentID
	lease.HandoffPeerID = peerID
	lease.HandoffUntil = now.Add(14 * time.Second)
	if lease.OwnerPeerID == peerID {
		lease.OwnerPeerID = ""
		lease.TakeoverUntil = time.Time{}
	}
	lease.LastUpdatedAt = now
	udpContentRouteState.leases[contentID] = lease
}

func udpContentRouteLeaseSnapshot(contentID string, now time.Time) (udpContentRouteLease, bool) {
	contentID = strings.TrimSpace(contentID)
	if contentID == "" {
		return udpContentRouteLease{}, false
	}
	udpContentRouteState.mu.Lock()
	defer udpContentRouteState.mu.Unlock()
	lease, ok := udpContentRouteState.leases[contentID]
	if !ok {
		return udpContentRouteLease{}, false
	}
	if (!lease.TakeoverUntil.IsZero() && now.After(lease.TakeoverUntil)) && (!lease.HandoffUntil.IsZero() && now.After(lease.HandoffUntil)) {
		delete(udpContentRouteState.leases, contentID)
		return udpContentRouteLease{}, false
	}
	if !lease.TakeoverUntil.IsZero() && now.After(lease.TakeoverUntil) {
		lease.OwnerPeerID = ""
		lease.TakeoverUntil = time.Time{}
	}
	if !lease.HandoffUntil.IsZero() && now.After(lease.HandoffUntil) {
		lease.HandoffPeerID = ""
		lease.HandoffUntil = time.Time{}
	}
	udpContentRouteState.leases[contentID] = lease
	return lease, true
}

func udpContentRouteTakeoverOwner(contentID string, now time.Time) string {
	lease, ok := udpContentRouteLeaseSnapshot(contentID, now)
	if !ok {
		return ""
	}
	if lease.TakeoverUntil.IsZero() || now.After(lease.TakeoverUntil) {
		return ""
	}
	return strings.TrimSpace(lease.OwnerPeerID)
}

func setUDPContentRouteTakeoverOwner(contentID string, ownerPeerID string, now time.Time) {
	contentID = strings.TrimSpace(contentID)
	ownerPeerID = strings.TrimSpace(ownerPeerID)
	if contentID == "" || ownerPeerID == "" {
		return
	}
	udpContentRouteState.mu.Lock()
	defer udpContentRouteState.mu.Unlock()
	lease := udpContentRouteState.leases[contentID]
	lease.ContentID = contentID
	lease.OwnerPeerID = ownerPeerID
	lease.TakeoverUntil = now.Add(12 * time.Second)
	lease.InflightLimit = suggestedUDPContentRouteInflightLimit(contentID, ownerPeerID, now)
	lease.LastUpdatedAt = now
	udpContentRouteState.leases[contentID] = lease
}

func udpContentRouteTakeoverAvailable(contentID string, peerID string, now time.Time) bool {
	lease, ok := udpContentRouteLeaseSnapshot(contentID, now)
	if !ok {
		return false
	}
	if strings.TrimSpace(lease.OwnerPeerID) != strings.TrimSpace(peerID) {
		return false
	}
	if lease.TakeoverUntil.IsZero() || now.After(lease.TakeoverUntil) {
		return false
	}
	limit := lease.InflightLimit
	if current := suggestedUDPContentRouteInflightLimit(contentID, peerID, now); current > 0 {
		limit = current
	}
	return lease.InflightCount < limit
}

func beginUDPContentRouteInflight(contentID string, peerID string, now time.Time) bool {
	contentID = strings.TrimSpace(contentID)
	peerID = strings.TrimSpace(peerID)
	if contentID == "" || peerID == "" {
		return false
	}
	udpContentRouteState.mu.Lock()
	defer udpContentRouteState.mu.Unlock()
	lease, ok := udpContentRouteState.leases[contentID]
	if !ok || strings.TrimSpace(lease.OwnerPeerID) != peerID || lease.TakeoverUntil.IsZero() || now.After(lease.TakeoverUntil) {
		return false
	}
	if current := suggestedUDPContentRouteInflightLimit(contentID, peerID, now); current > 0 {
		lease.InflightLimit = current
	}
	if lease.InflightCount >= lease.InflightLimit {
		udpContentRouteState.leases[contentID] = lease
		return false
	}
	lease.InflightCount++
	lease.LastUpdatedAt = now
	udpContentRouteState.leases[contentID] = lease
	return true
}

func finishUDPContentRouteInflight(contentID string, peerID string, now time.Time) {
	contentID = strings.TrimSpace(contentID)
	peerID = strings.TrimSpace(peerID)
	if contentID == "" || peerID == "" {
		return
	}
	udpContentRouteState.mu.Lock()
	defer udpContentRouteState.mu.Unlock()
	lease, ok := udpContentRouteState.leases[contentID]
	if !ok || strings.TrimSpace(lease.OwnerPeerID) != peerID {
		return
	}
	if lease.InflightCount > 0 {
		lease.InflightCount--
	}
	lease.LastUpdatedAt = now
	udpContentRouteState.leases[contentID] = lease
}

func suggestedUDPContentRouteInflightLimit(contentID string, peerID string, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 2
	}
	if udpSessionInHandoff(peerID, contentID, now) || strings.TrimSpace(session.State) == "cooling" {
		return 1
	}
	if session.RecommendedAttemptBudget >= 5 || (session.HealthScore >= 0.45 && session.SuccessStreak >= 2) {
		return 3
	}
	if session.RecommendedAttemptBudget <= 2 || session.HealthScore < 0 {
		return 1
	}
	return 2
}

func udpSessionOwnerRun(peerID string, contentID string, workerID int, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	if session.OwnerUntil.IsZero() || now.After(session.OwnerUntil) {
		return 0
	}
	if strings.TrimSpace(session.OwnerContentID) != strings.TrimSpace(contentID) {
		return 0
	}
	if workerID >= 0 && session.OwnerWorkerID != workerID {
		return 0
	}
	return session.ConsecutivePieceRuns
}

func udpSessionWindowBias(peerID string, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	bias := 0
	if session.RecommendedChunkWindow > 0 {
		bias = session.RecommendedChunkWindow - 4
	} else {
		switch udpSessionStateKind(session, now) {
		case "active":
			if session.HealthScore >= 0.45 {
				bias = 2
			} else {
				bias = 1
			}
		case "cooling":
			bias = -1
		}
	}
	if udpSessionPipelineActive(session, now) {
		bias += session.PipelineDepth
	}
	return bias
}

func udpSessionRoundTimeoutBias(peerID string, now time.Time) time.Duration {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	bias := time.Duration(0)
	if session.RecommendedRoundTimeout > 0 {
		bias = session.RecommendedRoundTimeout - time.Second
	} else {
		switch udpSessionStateKind(session, now) {
		case "active":
			if session.HealthScore >= 0.45 {
				bias = -200 * time.Millisecond
			} else {
				bias = -150 * time.Millisecond
			}
		case "warm":
			bias = -50 * time.Millisecond
		case "cooling":
			bias = 250 * time.Millisecond
		}
	}
	if udpSessionPipelineActive(session, now) {
		bias -= time.Duration(session.PipelineDepth) * 120 * time.Millisecond
	}
	return bias
}

func udpSessionAttemptBudgetBias(peerID string, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	bias := 0
	if session.RecommendedAttemptBudget > 0 {
		bias = session.RecommendedAttemptBudget - 3
	} else {
		switch udpSessionStateKind(session, now) {
		case "active":
			if session.HealthScore >= 0.45 {
				bias = 2
			} else {
				bias = 1
			}
		case "cooling":
			bias = -1
		}
	}
	if udpSessionPipelineActive(session, now) {
		bias += session.PipelineDepth
	}
	return bias
}

func sessionHasAddr(session udpPeerSession, remoteAddr string) bool {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return false
	}
	_, ok := session.Addresses[remoteAddr]
	return ok
}

func cloneUDPSession(session udpPeerSession) udpPeerSession {
	clone := session
	if session.Addresses != nil {
		clone.Addresses = make(map[string]time.Time, len(session.Addresses))
		for addr, seenAt := range session.Addresses {
			clone.Addresses[addr] = seenAt
		}
	}
	if session.RecentContentIDs != nil {
		clone.RecentContentIDs = make(map[string]time.Time, len(session.RecentContentIDs))
		for contentID, seenAt := range session.RecentContentIDs {
			clone.RecentContentIDs[contentID] = seenAt
		}
	}
	return clone
}

func refreshUDPSessionRecommendations(session *udpPeerSession, now time.Time) {
	if session == nil {
		return
	}
	state := udpSessionStateKind(*session, now)
	session.State = state
	switch state {
	case "active":
		session.RecommendedChunkWindow = 5
		session.RecommendedRoundTimeout = 850 * time.Millisecond
		session.RecommendedAttemptBudget = 4
		session.KeepaliveInterval = 6 * time.Second
		if session.HealthScore >= 0.45 {
			session.RecommendedChunkWindow = 6
			session.RecommendedRoundTimeout = 800 * time.Millisecond
			session.RecommendedAttemptBudget = 5
		}
	case "warm":
		session.RecommendedChunkWindow = 4
		session.RecommendedRoundTimeout = 950 * time.Millisecond
		session.RecommendedAttemptBudget = 3
		session.KeepaliveInterval = 10 * time.Second
	case "cooling":
		session.RecommendedChunkWindow = 3
		session.RecommendedRoundTimeout = 1250 * time.Millisecond
		session.RecommendedAttemptBudget = 2
		session.KeepaliveInterval = 15 * time.Second
	default:
		session.RecommendedChunkWindow = 4
		session.RecommendedRoundTimeout = time.Second
		session.RecommendedAttemptBudget = 3
		session.KeepaliveInterval = 15 * time.Second
	}
	session.RecommendedChunkWindow = clampSessionChunkWindow(session.RecommendedChunkWindow + session.ChunkWindowDelta)
	session.RecommendedRoundTimeout = clampSessionRoundTimeout(session.RecommendedRoundTimeout + session.RoundTimeoutDelta)
	session.RecommendedAttemptBudget = clampSessionAttemptBudget(session.RecommendedAttemptBudget + session.AttemptBudgetDelta)
	if udpSessionPipelineActive(*session, now) {
		session.RecommendedChunkWindow = clampSessionChunkWindow(session.RecommendedChunkWindow + session.PipelineDepth)
		session.RecommendedRoundTimeout = clampSessionRoundTimeout(session.RecommendedRoundTimeout - time.Duration(session.PipelineDepth)*120*time.Millisecond)
		session.RecommendedAttemptBudget = clampSessionAttemptBudget(session.RecommendedAttemptBudget + session.PipelineDepth)
	}
	if !session.LeaseUntil.IsZero() && now.Before(session.LeaseUntil) && session.ContentPieceRuns > 0 {
		session.RecommendedChunkWindow = clampSessionChunkWindow(session.RecommendedChunkWindow + minInt(session.ContentPieceRuns, 2))
		session.RecommendedRoundTimeout = clampSessionRoundTimeout(session.RecommendedRoundTimeout - time.Duration(minInt(session.ContentPieceRuns, 2))*60*time.Millisecond)
		session.RecommendedAttemptBudget = clampSessionAttemptBudget(session.RecommendedAttemptBudget + minInt(session.ContentPieceRuns, 2))
	}
	if !session.OwnerUntil.IsZero() && now.Before(session.OwnerUntil) && session.ConsecutivePieceRuns > 0 {
		session.RecommendedChunkWindow = clampSessionChunkWindow(session.RecommendedChunkWindow + minInt(session.ConsecutivePieceRuns, 2))
		session.RecommendedRoundTimeout = clampSessionRoundTimeout(session.RecommendedRoundTimeout - time.Duration(minInt(session.ConsecutivePieceRuns, 2))*80*time.Millisecond)
		session.RecommendedAttemptBudget = clampSessionAttemptBudget(session.RecommendedAttemptBudget + minInt(session.ConsecutivePieceRuns, 2))
	}
}

func updateUDPSessionPathAffinity(session *udpPeerSession, stage string, success bool, contentID string, now time.Time) {
	if session == nil {
		return
	}
	stage = strings.TrimSpace(stage)
	contentID = strings.TrimSpace(contentID)
	if success {
		if stage == "piece" {
			session.PreferredRole = "bulk"
			session.StickyContentID = contentID
			session.StickyUntil = now.Add(20 * time.Second)
			session.QuarantineUntil = time.Time{}
			return
		}
		if stage == "have" || stage == "probe" {
			if session.PreferredRole == "" {
				session.PreferredRole = "assist"
			}
			if session.StickyUntil.Before(now.Add(10 * time.Second)) {
				session.StickyUntil = now.Add(10 * time.Second)
			}
			if session.StickyContentID == "" {
				session.StickyContentID = contentID
			}
		}
		return
	}
	if stage == "piece" {
		session.PreferredRole = "fallback"
		session.StickyContentID = contentID
		session.StickyUntil = now.Add(12 * time.Second)
		if session.FailureStreak >= 2 || session.HealthScore <= -0.15 || strings.TrimSpace(session.LastErrorKind) != "" {
			session.QuarantineUntil = now.Add(18 * time.Second)
		}
		return
	}
	if (stage == "probe" || stage == "have") && session.FailureStreak >= 2 {
		if session.QuarantineUntil.Before(now.Add(10 * time.Second)) {
			session.QuarantineUntil = now.Add(10 * time.Second)
		}
	}
}

func udpSessionStateKind(session udpPeerSession, now time.Time) string {
	if session.FailureStreak >= 2 && now.Sub(session.LastFailureAt) <= 35*time.Second {
		return "cooling"
	}
	if session.HealthScore <= -0.2 && !session.LastFailureAt.IsZero() && now.Sub(session.LastFailureAt) <= 25*time.Second {
		return "cooling"
	}
	if !session.LastFailureAt.IsZero() && session.LastFailureAt.After(session.LastSuccessAt) && now.Sub(session.LastFailureAt) <= 25*time.Second && session.HealthScore < 0 {
		return "cooling"
	}
	if !session.LastSuccessAt.IsZero() && now.Sub(session.LastSuccessAt) <= 20*time.Second && session.HealthScore >= 0.08 {
		return "active"
	}
	if !session.LastSuccessAt.IsZero() && now.Sub(session.LastSuccessAt) <= 90*time.Second {
		return "warm"
	}
	return "idle"
}

func udpSessionStageDelta(stage string, success bool) float64 {
	var delta float64
	switch strings.TrimSpace(stage) {
	case "piece":
		delta = 0.25
	case "have":
		delta = 0.18
	case "probe":
		delta = 0.14
	case "keepalive":
		delta = 0.10
	default:
		delta = 0.10
	}
	if success {
		return delta
	}
	return -(delta * 1.2)
}

func clampUDPSessionHealth(value float64) float64 {
	if value > 1 {
		return 1
	}
	if value < -1 {
		return -1
	}
	return value
}

func clampSessionChunkWindow(window int) int {
	if window < 2 {
		return 2
	}
	if window > 7 {
		return 7
	}
	return window
}

func clampSessionRoundTimeout(timeout time.Duration) time.Duration {
	if timeout < 700*time.Millisecond {
		return 700 * time.Millisecond
	}
	if timeout > 1500*time.Millisecond {
		return 1500 * time.Millisecond
	}
	return timeout
}

func clampSessionAttemptBudget(budget int) int {
	if budget < 1 {
		return 1
	}
	if budget > 5 {
		return 5
	}
	return budget
}

func udpSessionPipelineActive(session udpPeerSession, now time.Time) bool {
	return session.PipelineDepth > 0 && !session.PipelineUntil.IsZero() && now.Before(session.PipelineUntil)
}

func maxSessionTime(left time.Time, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
