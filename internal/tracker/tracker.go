package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"generic-p2p/internal/core"
)

type PeerRecord struct {
	PeerID          string           `json:"peerId"`
	Addrs           []string         `json:"addrs"`
	UDPAddrs        []string         `json:"udpAddrs,omitempty"`
	ObservedAddr    string           `json:"observedAddr,omitempty"`
	ObservedUDPAddr string           `json:"observedUdpAddr,omitempty"`
	HaveRanges      []core.HaveRange `json:"haveRanges"`
	LastSeenAt      int64            `json:"lastSeenAt"`
}

type RegisterRequest struct {
	PeerID   string   `json:"peerId"`
	Addrs    []string `json:"addrs"`
	UDPAddrs []string `json:"udpAddrs,omitempty"`
}

type JoinSwarmRequest struct {
	PeerID     string           `json:"peerId"`
	ContentID  string           `json:"contentId"`
	HaveRanges []core.HaveRange `json:"haveRanges"`
}

type GetPeersResponse struct {
	ContentID string       `json:"contentId"`
	Peers     []PeerRecord `json:"peers"`
}

type RequestUDPProbeRequest struct {
	ContentID        string `json:"contentId"`
	RequesterPeerID  string `json:"requesterPeerId"`
	RequesterUDPAddr string `json:"requesterUdpAddr"`
	TargetPeerID     string `json:"targetPeerId"`
}

type UDPProbeTask struct {
	ContentID        string `json:"contentId"`
	RequesterPeerID  string `json:"requesterPeerId"`
	RequesterUDPAddr string `json:"requesterUdpAddr"`
	ObservedUDPAddr  string `json:"observedUdpAddr,omitempty"`
	RequestedAt      int64  `json:"requestedAt"`
}

type PollUDPProbeRequestsRequest struct {
	PeerID string `json:"peerId"`
}

type PollUDPProbeRequestsResponse struct {
	Requests []UDPProbeTask `json:"requests"`
}

type ReportUDPProbeResultRequest struct {
	TargetPeerID    string `json:"targetPeerId"`
	RequesterPeerID string `json:"requesterPeerId"`
	ContentID       string `json:"contentId"`
	Success         bool   `json:"success"`
	ErrorKind       string `json:"errorKind,omitempty"`
}

type ReportTransferPathRequest struct {
	TargetPeerID string `json:"targetPeerId"`
	ContentID    string `json:"contentId"`
	Transport    string `json:"transport"`
}

type ReportUDPKeepaliveResultRequest struct {
	TargetPeerID string `json:"targetPeerId"`
	ContentID    string `json:"contentId"`
	Success      bool   `json:"success"`
	ErrorKind    string `json:"errorKind,omitempty"`
}

type ReportUDPBurstProfileRequest struct {
	TargetPeerID  string `json:"targetPeerId"`
	ContentID     string `json:"contentId"`
	Profile       string `json:"profile"`
	LastOutcome   string `json:"lastOutcome,omitempty"`
	LastStage     string `json:"lastStage,omitempty"`
	FailureCount  int    `json:"failureCount"`
	LastOutcomeAt string `json:"lastOutcomeAt,omitempty"`
}

type ReportUDPDecisionRequest struct {
	TargetPeerID  string  `json:"targetPeerId"`
	ContentID     string  `json:"contentId"`
	BurstProfile  string  `json:"burstProfile,omitempty"`
	LastStage     string  `json:"lastStage,omitempty"`
	UDPBudget     int     `json:"udpBudget"`
	UDPTimeoutMs  int64   `json:"udpTimeoutMs"`
	SelectedScore float64 `json:"selectedScore"`
	Reason        string  `json:"reason,omitempty"`
}

type SwarmStatus struct {
	ContentID string       `json:"contentId"`
	PeerCount int          `json:"peerCount"`
	Peers     []PeerRecord `json:"peers"`
}

type PendingUDPProbeStatus struct {
	TargetPeerID string `json:"targetPeerId"`
	RequestCount int    `json:"requestCount"`
}

type UDPProbeResultStatus struct {
	TargetPeerID    string `json:"targetPeerId"`
	SuccessCount    int    `json:"successCount"`
	FailureCount    int    `json:"failureCount"`
	LastSuccessAt   int64  `json:"lastSuccessAt,omitempty"`
	LastFailureAt   int64  `json:"lastFailureAt,omitempty"`
	LastErrorKind   string `json:"lastErrorKind,omitempty"`
	LastRequesterID string `json:"lastRequesterPeerId,omitempty"`
	LastContentID   string `json:"lastContentId,omitempty"`
}

type PeerTransferPathStatus struct {
	TargetPeerID string `json:"targetPeerId"`
	ContentID    string `json:"contentId,omitempty"`
	LastPath     string `json:"lastPath,omitempty"`
	LastAt       int64  `json:"lastAt,omitempty"`
	UDPCount     int    `json:"udpCount"`
	TCPCount     int    `json:"tcpCount"`
}

type UDPKeepaliveStatus struct {
	TargetPeerID  string `json:"targetPeerId"`
	ContentID     string `json:"contentId,omitempty"`
	SuccessCount  int    `json:"successCount"`
	FailureCount  int    `json:"failureCount"`
	LastSuccessAt int64  `json:"lastSuccessAt,omitempty"`
	LastFailureAt int64  `json:"lastFailureAt,omitempty"`
	LastErrorKind string `json:"lastErrorKind,omitempty"`
}

type UDPBurstProfileStatus struct {
	TargetPeerID   string `json:"targetPeerId"`
	ContentID      string `json:"contentId,omitempty"`
	Profile        string `json:"profile"`
	LastOutcome    string `json:"lastOutcome,omitempty"`
	LastStage      string `json:"lastStage,omitempty"`
	FailureCount   int    `json:"failureCount"`
	LastReportedAt int64  `json:"lastReportedAt,omitempty"`
	LastOutcomeAt  string `json:"lastOutcomeAt,omitempty"`
}

type UDPDecisionStatus struct {
	TargetPeerID   string  `json:"targetPeerId"`
	ContentID      string  `json:"contentId,omitempty"`
	BurstProfile   string  `json:"burstProfile,omitempty"`
	LastStage      string  `json:"lastStage,omitempty"`
	UDPBudget      int     `json:"udpBudget"`
	UDPTimeoutMs   int64   `json:"udpTimeoutMs"`
	SelectedScore  float64 `json:"selectedScore"`
	Reason         string  `json:"reason,omitempty"`
	LastReportedAt int64   `json:"lastReportedAt,omitempty"`
	ReportCount    int     `json:"reportCount"`
}

type StatusResponse struct {
	GeneratedAt                 string                   `json:"generatedAt"`
	PeerCount                   int                      `json:"peerCount"`
	SwarmCount                  int                      `json:"swarmCount"`
	PendingUDPProbeCount        int                      `json:"pendingUdpProbeCount"`
	RecentUDPProbeSuccesses     int                      `json:"recentUdpProbeSuccesses"`
	RecentUDPProbeFailures      int                      `json:"recentUdpProbeFailures"`
	RecentUDPKeepaliveSuccesses int                      `json:"recentUdpKeepaliveSuccesses"`
	RecentUDPKeepaliveFailures  int                      `json:"recentUdpKeepaliveFailures"`
	PeerTTLSeconds              int                      `json:"peerTtlSeconds"`
	CleanupIntervalSeconds      int                      `json:"cleanupIntervalSeconds"`
	StatePath                   string                   `json:"statePath,omitempty"`
	PendingUDPProbes            []PendingUDPProbeStatus  `json:"pendingUdpProbes,omitempty"`
	UDPProbeResults             []UDPProbeResultStatus   `json:"udpProbeResults,omitempty"`
	PeerTransferPaths           []PeerTransferPathStatus `json:"peerTransferPaths,omitempty"`
	UDPKeepaliveResults         []UDPKeepaliveStatus     `json:"udpKeepaliveResults,omitempty"`
	UDPBurstProfiles            []UDPBurstProfileStatus  `json:"udpBurstProfiles,omitempty"`
	UDPDecisions                []UDPDecisionStatus      `json:"udpDecisions,omitempty"`
	Swarms                      []SwarmStatus            `json:"swarms"`
}

type udpProbeResultSummary struct {
	SuccessCount    int
	FailureCount    int
	LastSuccessAt   int64
	LastFailureAt   int64
	LastErrorKind   string
	LastRequesterID string
	LastContentID   string
}

type peerTransferPathSummary struct {
	ContentID string
	LastPath  string
	LastAt    int64
	UDPCount  int
	TCPCount  int
}

type udpKeepaliveSummary struct {
	ContentID     string
	SuccessCount  int
	FailureCount  int
	LastSuccessAt int64
	LastFailureAt int64
	LastErrorKind string
}

type udpBurstProfileSummary struct {
	ContentID      string
	Profile        string
	LastOutcome    string
	LastStage      string
	FailureCount   int
	LastReportedAt int64
	LastOutcomeAt  string
}

type udpDecisionSummary struct {
	ContentID      string
	BurstProfile   string
	LastStage      string
	UDPBudget      int
	UDPTimeoutMs   int64
	SelectedScore  float64
	Reason         string
	LastReportedAt int64
	ReportCount    int
}

type Server struct {
	mu               sync.Mutex
	peers            map[string]PeerRecord
	swarms           map[string]map[string]bool
	udpProbes        map[string][]UDPProbeTask
	udpProbeResults  map[string]udpProbeResultSummary
	peerTransfers    map[string]peerTransferPathSummary
	udpKeepalives    map[string]udpKeepaliveSummary
	udpBurstProfiles map[string]udpBurstProfileSummary
	udpDecisions     map[string]udpDecisionSummary
	peerTTL          time.Duration
	cleanupInterval  time.Duration
	statePath        string
	webDataDir       string
	webUsersPath     string
	webUsers         map[string]string
	webSessions      map[string]string
}

func NewServer() *Server {
	return &Server{
		peers:            make(map[string]PeerRecord),
		swarms:           make(map[string]map[string]bool),
		udpProbes:        make(map[string][]UDPProbeTask),
		udpProbeResults:  make(map[string]udpProbeResultSummary),
		peerTransfers:    make(map[string]peerTransferPathSummary),
		udpKeepalives:    make(map[string]udpKeepaliveSummary),
		udpBurstProfiles: make(map[string]udpBurstProfileSummary),
		udpDecisions:     make(map[string]udpDecisionSummary),
		peerTTL:          10 * time.Second,
		cleanupInterval:  2 * time.Second,
		webDataDir:       defaultWebDataDir,
		webSessions:      make(map[string]string),
	}
}

func (s *Server) WithStatePath(statePath string) *Server {
	s.statePath = statePath
	return s
}

func (s *Server) WithPeerTTL(peerTTL time.Duration) *Server {
	s.peerTTL = peerTTL
	return s
}

func (s *Server) WithCleanupInterval(cleanupInterval time.Duration) *Server {
	s.cleanupInterval = cleanupInterval
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/peers/register", s.handleRegister)
	mux.HandleFunc("/v1/udp/probes/request", s.handleRequestUDPProbe)
	mux.HandleFunc("/v1/udp/probes/poll", s.handlePollUDPProbeRequests)
	mux.HandleFunc("/v1/udp/probes/report", s.handleReportUDPProbeResult)
	mux.HandleFunc("/v1/udp/keepalives/report", s.handleReportUDPKeepaliveResult)
	mux.HandleFunc("/v1/udp/burst-profiles/report", s.handleReportUDPBurstProfile)
	mux.HandleFunc("/v1/udp/decisions/report", s.handleReportUDPDecision)
	mux.HandleFunc("/v1/transfers/report", s.handleReportTransferPath)
	mux.HandleFunc("/v1/swarms/join", s.handleJoin)
	mux.HandleFunc("/v1/swarms/", s.handleGetPeers)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1/web/shares/", s.handleWebShareFile)
	mux.HandleFunc("/v1/web/shares", s.handleWebShares)
	mux.HandleFunc("/login", s.handleWebLogin)
	mux.HandleFunc("/logout", s.handleWebLogout)
	mux.HandleFunc("/", s.handleWebApp)
	return mux
}

func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	if err := s.loadState(); err != nil {
		return err
	}
	if err := s.loadWebUsers(); err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
	}

	if s.cleanupInterval > 0 {
		go s.cleanupLoop(ctx)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	err := httpServer.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.PeerID) == "" || len(req.Addrs) == 0 {
		http.Error(w, "peerId and addrs are required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	record := s.peers[req.PeerID]
	record.PeerID = req.PeerID
	record.Addrs = req.Addrs
	record.UDPAddrs = req.UDPAddrs
	record.ObservedAddr = observedAddr(r, firstAddr(req.Addrs))
	record.ObservedUDPAddr = observedAddr(r, firstAddr(req.UDPAddrs))
	record.LastSeenAt = time.Now().Unix()
	s.peers[req.PeerID] = record
	if err := s.saveLocked(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req JoinSwarmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.PeerID) == "" || strings.TrimSpace(req.ContentID) == "" {
		http.Error(w, "peerId and contentId are required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	record := s.peers[req.PeerID]
	record.PeerID = req.PeerID
	record.HaveRanges = req.HaveRanges
	record.LastSeenAt = time.Now().Unix()
	s.peers[req.PeerID] = record

	if s.swarms[req.ContentID] == nil {
		s.swarms[req.ContentID] = make(map[string]bool)
	}
	s.swarms[req.ContentID][req.PeerID] = true
	if err := s.saveLocked(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleGetPeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !strings.HasSuffix(r.URL.Path, "/peers") {
		http.NotFound(w, r)
		return
	}

	prefix := "/v1/swarms/"
	contentID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), "/peers")
	contentID = strings.Trim(contentID, "/")
	if contentID == "" {
		http.Error(w, "contentId is required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	peerIDs := s.swarms[contentID]
	response := GetPeersResponse{
		ContentID: contentID,
		Peers:     make([]PeerRecord, 0, len(peerIDs)),
	}
	for peerID := range peerIDs {
		record := s.peers[peerID]
		if record.PeerID == "" {
			continue
		}
		response.Peers = append(response.Peers, record)
	}
	writeJSON(w, response)
}

func (s *Server) handleRequestUDPProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RequestUDPProbeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.ContentID) == "" ||
		strings.TrimSpace(req.RequesterPeerID) == "" ||
		strings.TrimSpace(req.RequesterUDPAddr) == "" ||
		strings.TrimSpace(req.TargetPeerID) == "" {
		http.Error(w, "contentId, requesterPeerId, requesterUdpAddr, and targetPeerId are required", http.StatusBadRequest)
		return
	}
	if req.RequesterPeerID == req.TargetPeerID {
		http.Error(w, "targetPeerId must differ from requesterPeerId", http.StatusBadRequest)
		return
	}

	now := time.Now()
	task := UDPProbeTask{
		ContentID:        req.ContentID,
		RequesterPeerID:  req.RequesterPeerID,
		RequesterUDPAddr: req.RequesterUDPAddr,
		ObservedUDPAddr:  observedAddr(r, req.RequesterUDPAddr),
		RequestedAt:      now.Unix(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(now)
	s.udpProbes[req.TargetPeerID] = appendUDPProbeTask(s.udpProbes[req.TargetPeerID], task)

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handlePollUDPProbeRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PollUDPProbeRequestsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.PeerID) == "" {
		http.Error(w, "peerId is required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	requests := append([]UDPProbeTask(nil), s.udpProbes[req.PeerID]...)
	delete(s.udpProbes, req.PeerID)
	writeJSON(w, PollUDPProbeRequestsResponse{Requests: requests})
}

func (s *Server) handleReportUDPProbeResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReportUDPProbeResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.TargetPeerID) == "" || strings.TrimSpace(req.RequesterPeerID) == "" || strings.TrimSpace(req.ContentID) == "" {
		http.Error(w, "targetPeerId, requesterPeerId, and contentId are required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	summary := s.udpProbeResults[req.TargetPeerID]
	if req.Success {
		summary.SuccessCount++
		summary.LastSuccessAt = time.Now().Unix()
		summary.LastErrorKind = ""
	} else {
		summary.FailureCount++
		summary.LastFailureAt = time.Now().Unix()
		summary.LastErrorKind = req.ErrorKind
	}
	summary.LastRequesterID = req.RequesterPeerID
	summary.LastContentID = req.ContentID
	s.udpProbeResults[req.TargetPeerID] = summary

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleReportTransferPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReportTransferPathRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.TargetPeerID) == "" || strings.TrimSpace(req.ContentID) == "" {
		http.Error(w, "targetPeerId and contentId are required", http.StatusBadRequest)
		return
	}

	transport := strings.ToLower(strings.TrimSpace(req.Transport))
	if transport != "udp" && transport != "tcp" {
		http.Error(w, "transport must be udp or tcp", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	summary := s.peerTransfers[req.TargetPeerID]
	summary.ContentID = req.ContentID
	summary.LastPath = transport
	summary.LastAt = time.Now().Unix()
	switch transport {
	case "udp":
		summary.UDPCount++
	case "tcp":
		summary.TCPCount++
	}
	s.peerTransfers[req.TargetPeerID] = summary

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleReportUDPKeepaliveResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReportUDPKeepaliveResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.TargetPeerID) == "" || strings.TrimSpace(req.ContentID) == "" {
		http.Error(w, "targetPeerId and contentId are required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	summary := s.udpKeepalives[req.TargetPeerID]
	summary.ContentID = req.ContentID
	if req.Success {
		summary.SuccessCount++
		summary.LastSuccessAt = time.Now().Unix()
		summary.LastErrorKind = ""
	} else {
		summary.FailureCount++
		summary.LastFailureAt = time.Now().Unix()
		summary.LastErrorKind = req.ErrorKind
	}
	s.udpKeepalives[req.TargetPeerID] = summary

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleReportUDPBurstProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReportUDPBurstProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.TargetPeerID) == "" || strings.TrimSpace(req.ContentID) == "" || strings.TrimSpace(req.Profile) == "" {
		http.Error(w, "targetPeerId, contentId, and profile are required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	s.udpBurstProfiles[req.TargetPeerID] = udpBurstProfileSummary{
		ContentID:      req.ContentID,
		Profile:        req.Profile,
		LastOutcome:    req.LastOutcome,
		LastStage:      req.LastStage,
		FailureCount:   req.FailureCount,
		LastReportedAt: time.Now().Unix(),
		LastOutcomeAt:  req.LastOutcomeAt,
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleReportUDPDecision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReportUDPDecisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.TargetPeerID) == "" || strings.TrimSpace(req.ContentID) == "" {
		http.Error(w, "targetPeerId and contentId are required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())

	summary := s.udpDecisions[req.TargetPeerID]
	summary.ContentID = req.ContentID
	summary.BurstProfile = req.BurstProfile
	summary.LastStage = req.LastStage
	summary.UDPBudget = req.UDPBudget
	summary.UDPTimeoutMs = req.UDPTimeoutMs
	summary.SelectedScore = req.SelectedScore
	summary.Reason = req.Reason
	summary.LastReportedAt = time.Now().Unix()
	summary.ReportCount++
	s.udpDecisions[req.TargetPeerID] = summary

	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.mu.Lock()
			s.pruneExpiredLocked(now)
			_ = s.saveLocked()
			s.mu.Unlock()
		}
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.webAuthEnabled() && strings.HasPrefix(r.Header.Get("Referer"), "http") && !s.requireWebAuth(w, r) {
		return
	}

	writeJSON(w, s.Status())
}

func (s *Server) Status() StatusResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.pruneExpiredLocked(now)

	response := StatusResponse{
		GeneratedAt:            now.Format(time.RFC3339),
		PeerCount:              len(s.peers),
		SwarmCount:             len(s.swarms),
		PendingUDPProbes:       make([]PendingUDPProbeStatus, 0, len(s.udpProbes)),
		UDPProbeResults:        make([]UDPProbeResultStatus, 0, len(s.udpProbeResults)),
		PeerTransferPaths:      make([]PeerTransferPathStatus, 0, len(s.peerTransfers)),
		UDPKeepaliveResults:    make([]UDPKeepaliveStatus, 0, len(s.udpKeepalives)),
		UDPBurstProfiles:       make([]UDPBurstProfileStatus, 0, len(s.udpBurstProfiles)),
		UDPDecisions:           make([]UDPDecisionStatus, 0, len(s.udpDecisions)),
		PeerTTLSeconds:         int(s.peerTTL / time.Second),
		CleanupIntervalSeconds: int(s.cleanupInterval / time.Second),
		StatePath:              s.statePath,
		Swarms:                 make([]SwarmStatus, 0, len(s.swarms)),
	}

	for contentID, peerSet := range s.swarms {
		swarm := SwarmStatus{
			ContentID: contentID,
			PeerCount: len(peerSet),
			Peers:     make([]PeerRecord, 0, len(peerSet)),
		}
		for peerID := range peerSet {
			record := s.peers[peerID]
			if record.PeerID == "" {
				continue
			}
			swarm.Peers = append(swarm.Peers, record)
		}
		response.Swarms = append(response.Swarms, swarm)
	}
	for targetPeerID, tasks := range s.udpProbes {
		if len(tasks) == 0 {
			continue
		}
		response.PendingUDPProbeCount += len(tasks)
		response.PendingUDPProbes = append(response.PendingUDPProbes, PendingUDPProbeStatus{
			TargetPeerID: targetPeerID,
			RequestCount: len(tasks),
		})
	}
	for targetPeerID, summary := range s.udpProbeResults {
		response.RecentUDPProbeSuccesses += summary.SuccessCount
		response.RecentUDPProbeFailures += summary.FailureCount
		response.UDPProbeResults = append(response.UDPProbeResults, UDPProbeResultStatus{
			TargetPeerID:    targetPeerID,
			SuccessCount:    summary.SuccessCount,
			FailureCount:    summary.FailureCount,
			LastSuccessAt:   summary.LastSuccessAt,
			LastFailureAt:   summary.LastFailureAt,
			LastErrorKind:   summary.LastErrorKind,
			LastRequesterID: summary.LastRequesterID,
			LastContentID:   summary.LastContentID,
		})
	}
	for targetPeerID, summary := range s.peerTransfers {
		response.PeerTransferPaths = append(response.PeerTransferPaths, PeerTransferPathStatus{
			TargetPeerID: targetPeerID,
			ContentID:    summary.ContentID,
			LastPath:     summary.LastPath,
			LastAt:       summary.LastAt,
			UDPCount:     summary.UDPCount,
			TCPCount:     summary.TCPCount,
		})
	}
	for targetPeerID, summary := range s.udpKeepalives {
		response.RecentUDPKeepaliveSuccesses += summary.SuccessCount
		response.RecentUDPKeepaliveFailures += summary.FailureCount
		response.UDPKeepaliveResults = append(response.UDPKeepaliveResults, UDPKeepaliveStatus{
			TargetPeerID:  targetPeerID,
			ContentID:     summary.ContentID,
			SuccessCount:  summary.SuccessCount,
			FailureCount:  summary.FailureCount,
			LastSuccessAt: summary.LastSuccessAt,
			LastFailureAt: summary.LastFailureAt,
			LastErrorKind: summary.LastErrorKind,
		})
	}
	for targetPeerID, summary := range s.udpBurstProfiles {
		response.UDPBurstProfiles = append(response.UDPBurstProfiles, UDPBurstProfileStatus{
			TargetPeerID:   targetPeerID,
			ContentID:      summary.ContentID,
			Profile:        summary.Profile,
			LastOutcome:    summary.LastOutcome,
			LastStage:      summary.LastStage,
			FailureCount:   summary.FailureCount,
			LastReportedAt: summary.LastReportedAt,
			LastOutcomeAt:  summary.LastOutcomeAt,
		})
	}
	for targetPeerID, summary := range s.udpDecisions {
		response.UDPDecisions = append(response.UDPDecisions, UDPDecisionStatus{
			TargetPeerID:   targetPeerID,
			ContentID:      summary.ContentID,
			BurstProfile:   summary.BurstProfile,
			LastStage:      summary.LastStage,
			UDPBudget:      summary.UDPBudget,
			UDPTimeoutMs:   summary.UDPTimeoutMs,
			SelectedScore:  summary.SelectedScore,
			Reason:         summary.Reason,
			LastReportedAt: summary.LastReportedAt,
			ReportCount:    summary.ReportCount,
		})
	}

	return response
}

func (s *Server) pruneExpiredLocked(now time.Time) {
	if s.peerTTL <= 0 {
		return
	}

	expiredPeerIDs := make([]string, 0)
	for peerID, record := range s.peers {
		lastSeenAt := time.Unix(record.LastSeenAt, 0)
		if now.Sub(lastSeenAt) > s.peerTTL {
			expiredPeerIDs = append(expiredPeerIDs, peerID)
		}
	}

	for _, peerID := range expiredPeerIDs {
		delete(s.peers, peerID)
		delete(s.udpProbes, peerID)
		delete(s.udpProbeResults, peerID)
		delete(s.peerTransfers, peerID)
		delete(s.udpKeepalives, peerID)
		delete(s.udpBurstProfiles, peerID)
		delete(s.udpDecisions, peerID)
		for contentID, peerSet := range s.swarms {
			delete(peerSet, peerID)
			if len(peerSet) == 0 {
				delete(s.swarms, contentID)
			}
		}
	}

	for peerID, probes := range s.udpProbes {
		kept := probes[:0]
		for _, probe := range probes {
			if now.Sub(time.Unix(probe.RequestedAt, 0)) <= s.peerTTL {
				kept = append(kept, probe)
			}
		}
		if len(kept) == 0 {
			delete(s.udpProbes, peerID)
			continue
		}
		s.udpProbes[peerID] = kept
	}
}

func firstAddr(addrs []string) string {
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			return addr
		}
	}
	return ""
}

func observedAddr(r *http.Request, declaredAddr string) string {
	if declaredAddr == "" {
		return ""
	}
	remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ""
	}
	_, declaredPort, err := net.SplitHostPort(declaredAddr)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(remoteHost, declaredPort)
}

func appendUDPProbeTask(tasks []UDPProbeTask, task UDPProbeTask) []UDPProbeTask {
	for i, existing := range tasks {
		if existing.ContentID == task.ContentID &&
			existing.RequesterPeerID == task.RequesterPeerID &&
			existing.RequesterUDPAddr == task.RequesterUDPAddr {
			tasks[i] = task
			return tasks
		}
	}
	return append(tasks, task)
}

type persistedState struct {
	Peers  map[string]PeerRecord `json:"peers"`
	Swarms map[string][]string   `json:"swarms"`
}

func (s *Server) loadState() error {
	if strings.TrimSpace(s.statePath) == "" {
		return nil
	}

	data, err := os.ReadFile(s.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if state.Peers != nil {
		s.peers = state.Peers
	}
	if state.Swarms != nil {
		s.swarms = make(map[string]map[string]bool, len(state.Swarms))
		for contentID, peerIDs := range state.Swarms {
			peerSet := make(map[string]bool, len(peerIDs))
			for _, peerID := range peerIDs {
				peerSet[peerID] = true
			}
			s.swarms[contentID] = peerSet
		}
	}
	s.pruneExpiredLocked(time.Now())
	return nil
}

func (s *Server) saveLocked() error {
	if strings.TrimSpace(s.statePath) == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o755); err != nil {
		return err
	}

	state := persistedState{
		Peers:  s.peers,
		Swarms: make(map[string][]string, len(s.swarms)),
	}
	for contentID, peerSet := range s.swarms {
		peerIDs := make([]string, 0, len(peerSet))
		for peerID := range peerSet {
			peerIDs = append(peerIDs, peerID)
		}
		state.Swarms[contentID] = peerIDs
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.statePath, data, 0o644)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) RegisterPeer(ctx context.Context, peerID string, addrs []string) error {
	return c.RegisterPeerWithUDP(ctx, peerID, addrs, nil)
}

func (c *Client) RegisterPeerWithUDP(ctx context.Context, peerID string, addrs []string, udpAddrs []string) error {
	reqBody := RegisterRequest{
		PeerID:   peerID,
		Addrs:    addrs,
		UDPAddrs: udpAddrs,
	}
	return c.postJSON(ctx, "/v1/peers/register", reqBody, nil)
}

func (c *Client) JoinSwarm(ctx context.Context, peerID string, contentID string, haveRanges []core.HaveRange) error {
	reqBody := JoinSwarmRequest{
		PeerID:     peerID,
		ContentID:  contentID,
		HaveRanges: haveRanges,
	}
	return c.postJSON(ctx, "/v1/swarms/join", reqBody, nil)
}

func (c *Client) GetPeers(ctx context.Context, contentID string) ([]PeerRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/swarms/"+contentID+"/peers", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker get peers returned %s", resp.Status)
	}

	var body GetPeersResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Peers, nil
}

func (c *Client) RequestUDPProbe(ctx context.Context, contentID string, requesterPeerID string, requesterUDPAddr string, targetPeerID string) error {
	reqBody := RequestUDPProbeRequest{
		ContentID:        contentID,
		RequesterPeerID:  requesterPeerID,
		RequesterUDPAddr: requesterUDPAddr,
		TargetPeerID:     targetPeerID,
	}
	return c.postJSON(ctx, "/v1/udp/probes/request", reqBody, nil)
}

func (c *Client) PollUDPProbeRequests(ctx context.Context, peerID string) ([]UDPProbeTask, error) {
	reqBody := PollUDPProbeRequestsRequest{PeerID: peerID}
	var response PollUDPProbeRequestsResponse
	if err := c.postJSON(ctx, "/v1/udp/probes/poll", reqBody, &response); err != nil {
		return nil, err
	}
	return response.Requests, nil
}

func (c *Client) ReportUDPProbeResult(ctx context.Context, targetPeerID string, requesterPeerID string, contentID string, success bool, errorKind string) error {
	reqBody := ReportUDPProbeResultRequest{
		TargetPeerID:    targetPeerID,
		RequesterPeerID: requesterPeerID,
		ContentID:       contentID,
		Success:         success,
		ErrorKind:       errorKind,
	}
	return c.postJSON(ctx, "/v1/udp/probes/report", reqBody, nil)
}

func (c *Client) ReportTransferPath(ctx context.Context, targetPeerID string, contentID string, transport string) error {
	reqBody := ReportTransferPathRequest{
		TargetPeerID: targetPeerID,
		ContentID:    contentID,
		Transport:    transport,
	}
	return c.postJSON(ctx, "/v1/transfers/report", reqBody, nil)
}

func (c *Client) ReportUDPKeepaliveResult(ctx context.Context, targetPeerID string, contentID string, success bool, errorKind string) error {
	reqBody := ReportUDPKeepaliveResultRequest{
		TargetPeerID: targetPeerID,
		ContentID:    contentID,
		Success:      success,
		ErrorKind:    errorKind,
	}
	return c.postJSON(ctx, "/v1/udp/keepalives/report", reqBody, nil)
}

func (c *Client) ReportUDPBurstProfile(ctx context.Context, targetPeerID string, contentID string, profile string, lastOutcome string, lastStage string, failureCount int, lastOutcomeAt string) error {
	reqBody := ReportUDPBurstProfileRequest{
		TargetPeerID:  targetPeerID,
		ContentID:     contentID,
		Profile:       profile,
		LastOutcome:   lastOutcome,
		LastStage:     lastStage,
		FailureCount:  failureCount,
		LastOutcomeAt: lastOutcomeAt,
	}
	return c.postJSON(ctx, "/v1/udp/burst-profiles/report", reqBody, nil)
}

func (c *Client) ReportUDPDecision(ctx context.Context, targetPeerID string, contentID string, burstProfile string, lastStage string, udpBudget int, udpTimeoutMs int64, selectedScore float64, reason string) error {
	reqBody := ReportUDPDecisionRequest{
		TargetPeerID:  targetPeerID,
		ContentID:     contentID,
		BurstProfile:  burstProfile,
		LastStage:     lastStage,
		UDPBudget:     udpBudget,
		UDPTimeoutMs:  udpTimeoutMs,
		SelectedScore: selectedScore,
		Reason:        reason,
	}
	return c.postJSON(ctx, "/v1/udp/decisions/report", reqBody, nil)
}

func (c *Client) GetStatus(ctx context.Context) (StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/status", nil)
	if err != nil {
		return StatusResponse{}, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return StatusResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return StatusResponse{}, fmt.Errorf("tracker get status returned %s", resp.Status)
	}

	var body StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return StatusResponse{}, err
	}
	return body, nil
}

func (c *Client) postJSON(ctx context.Context, path string, requestBody any, responseBody any) error {
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tracker post %s returned %s", path, resp.Status)
	}
	if responseBody != nil {
		return json.NewDecoder(resp.Body).Decode(responseBody)
	}
	return nil
}
