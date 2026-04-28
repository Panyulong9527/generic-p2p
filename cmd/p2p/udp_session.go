package main

import (
	"sort"
	"strings"
	"sync"
	"time"
)

type udpPeerSession struct {
	PeerID                   string
	PrimaryAddr              string
	Addresses                map[string]time.Time
	State                    string
	HealthScore              float64
	LastStage                string
	LastErrorKind            string
	SuccessStreak            int
	FailureStreak            int
	RecommendedChunkWindow   int
	RecommendedRoundTimeout  time.Duration
	RecommendedAttemptBudget int
	KeepaliveInterval        time.Duration
	LastActiveAt             time.Time
	LastSuccessAt            time.Time
	LastFailureAt            time.Time
	LastKeepaliveAt          time.Time
	LastObservedAt           time.Time
	ObservedSource           string
	RecentContentIDs         map[string]time.Time
}

var udpSessionState = struct {
	mu       sync.Mutex
	sessions map[string]udpPeerSession
}{
	sessions: make(map[string]udpPeerSession),
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

func udpSessionWindowBias(peerID string, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	if session.RecommendedChunkWindow > 0 {
		return session.RecommendedChunkWindow - 4
	}
	switch udpSessionStateKind(session, now) {
	case "active":
		if session.HealthScore >= 0.45 {
			return 2
		}
		return 1
	case "cooling":
		return -1
	default:
		return 0
	}
}

func udpSessionRoundTimeoutBias(peerID string, now time.Time) time.Duration {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	if session.RecommendedRoundTimeout > 0 {
		return session.RecommendedRoundTimeout - time.Second
	}
	switch udpSessionStateKind(session, now) {
	case "active":
		if session.HealthScore >= 0.45 {
			return -200 * time.Millisecond
		}
		return -150 * time.Millisecond
	case "warm":
		return -50 * time.Millisecond
	case "cooling":
		return 250 * time.Millisecond
	default:
		return 0
	}
}

func udpSessionAttemptBudgetBias(peerID string, now time.Time) int {
	session, ok := udpSessionSnapshot(peerID, now)
	if !ok {
		return 0
	}
	if session.RecommendedAttemptBudget > 0 {
		return session.RecommendedAttemptBudget - 3
	}
	switch udpSessionStateKind(session, now) {
	case "active":
		if session.HealthScore >= 0.45 {
			return 2
		}
		return 1
	case "cooling":
		return -1
	default:
		return 0
	}
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

func maxSessionTime(left time.Time, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}
