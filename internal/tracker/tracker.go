package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

type Server struct {
	mu     sync.Mutex
	peers  map[string]PeerRecord
	swarms map[string]map[string]bool
}

func NewServer() *Server {
	return &Server{
		peers:  make(map[string]PeerRecord),
		swarms: make(map[string]map[string]bool),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/peers/register", s.handleRegister)
	mux.HandleFunc("/v1/swarms/join", s.handleJoin)
	mux.HandleFunc("/v1/swarms/", s.handleGetPeers)
	return mux
}

func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
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

	record := s.peers[req.PeerID]
	record.PeerID = req.PeerID
	record.Addrs = req.Addrs
	record.LastSeenAt = time.Now().Unix()
	s.peers[req.PeerID] = record

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

	record := s.peers[req.PeerID]
	record.PeerID = req.PeerID
	record.HaveRanges = req.HaveRanges
	record.LastSeenAt = time.Now().Unix()
	s.peers[req.PeerID] = record

	if s.swarms[req.ContentID] == nil {
		s.swarms[req.ContentID] = make(map[string]bool)
	}
	s.swarms[req.ContentID][req.PeerID] = true

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
