package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"generic-p2p/internal/core"
)

type PeerRecord struct {
	PeerID     string           `json:"peerId"`
	Addrs      []string         `json:"addrs"`
	HaveRanges []core.HaveRange `json:"haveRanges"`
	LastSeenAt int64            `json:"lastSeenAt"`
}

type RegisterRequest struct {
	PeerID string   `json:"peerId"`
	Addrs  []string `json:"addrs"`
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

type SwarmStatus struct {
	ContentID string       `json:"contentId"`
	PeerCount int          `json:"peerCount"`
	Peers     []PeerRecord `json:"peers"`
}

type StatusResponse struct {
	GeneratedAt            string        `json:"generatedAt"`
	PeerCount              int           `json:"peerCount"`
	SwarmCount             int           `json:"swarmCount"`
	PeerTTLSeconds         int           `json:"peerTtlSeconds"`
	CleanupIntervalSeconds int           `json:"cleanupIntervalSeconds"`
	StatePath              string        `json:"statePath,omitempty"`
	Swarms                 []SwarmStatus `json:"swarms"`
}

type Server struct {
	mu              sync.Mutex
	peers           map[string]PeerRecord
	swarms          map[string]map[string]bool
	peerTTL         time.Duration
	cleanupInterval time.Duration
	statePath       string
	webDataDir      string
}

func NewServer() *Server {
	return &Server{
		peers:           make(map[string]PeerRecord),
		swarms:          make(map[string]map[string]bool),
		peerTTL:         10 * time.Second,
		cleanupInterval: 2 * time.Second,
		webDataDir:      defaultWebDataDir,
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
	mux.HandleFunc("/v1/swarms/join", s.handleJoin)
	mux.HandleFunc("/v1/swarms/", s.handleGetPeers)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1/web/shares/", s.handleWebShareFile)
	mux.HandleFunc("/v1/web/shares", s.handleWebShares)
	mux.HandleFunc("/", s.handleWebApp)
	return mux
}

func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	if err := s.loadState(); err != nil {
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
		for contentID, peerSet := range s.swarms {
			delete(peerSet, peerID)
			if len(peerSet) == 0 {
				delete(s.swarms, contentID)
			}
		}
	}
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
	reqBody := RegisterRequest{
		PeerID: peerID,
		Addrs:  addrs,
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
