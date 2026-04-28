package net

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"generic-p2p/internal/core"
)

const (
	UDPMessageTypePing         = "UDP_PING"
	UDPMessageTypePong         = "UDP_PONG"
	UDPMessageTypeHaveRequest  = "UDP_HAVE_REQUEST"
	UDPMessageTypeHaveResponse = "UDP_HAVE_RESPONSE"
	UDPMessageTypePieceRequest = "UDP_PIECE_REQUEST"
	UDPMessageTypePieceChunk   = "UDP_PIECE_CHUNK"
	UDPMessageTypeError        = "UDP_ERROR"

	udpMaxDatagramBytes = 4096
	udpChunkDataBytes   = 900
	udpPieceChunkWindow = 4
)

type UDPMessage struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body,omitempty"`
}

type UDPPing struct {
	RequestID string `json:"requestId"`
	PeerID    string `json:"peerId,omitempty"`
	ContentID string `json:"contentId,omitempty"`
}

type UDPPong struct {
	RequestID string `json:"requestId"`
}

type UDPHaveRequest struct {
	RequestID string `json:"requestId"`
	ContentID string `json:"contentId"`
}

type UDPHaveResponse struct {
	RequestID  string           `json:"requestId"`
	ContentID  string           `json:"contentId"`
	HaveRanges []core.HaveRange `json:"haveRanges"`
}

type UDPPieceRequest struct {
	RequestID    string `json:"requestId"`
	ContentID    string `json:"contentId"`
	PieceIndex   int    `json:"pieceIndex"`
	ChunkIndexes []int  `json:"chunkIndexes,omitempty"`
}

type UDPPieceChunk struct {
	RequestID   string `json:"requestId"`
	ContentID   string `json:"contentId"`
	PieceIndex  int    `json:"pieceIndex"`
	ChunkIndex  int    `json:"chunkIndex"`
	TotalChunks int    `json:"totalChunks"`
	Data        []byte `json:"data"`
}

type UDPError struct {
	RequestID string `json:"requestId"`
	Message   string `json:"message"`
}

type UDPTimeoutError struct {
	Op   string
	Addr string
	Err  error
}

func (e *UDPTimeoutError) Error() string {
	if e == nil {
		return "udp timeout"
	}
	if e.Addr == "" {
		return fmt.Sprintf("%s timed out: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("%s %s timed out: %v", e.Op, e.Addr, e.Err)
}

func (e *UDPTimeoutError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type observedUDPPeer struct {
	Addr   string
	SeenAt time.Time
}

type recentUDPSuccess struct {
	Addr   string
	SeenAt time.Time
}

type udpPendingRequest struct {
	ch chan UDPMessage
}

type udpSharedSocket struct {
	conn    *net.UDPConn
	mu      sync.Mutex
	wait    map[string]udpPendingRequest
	rawWait map[string]chan []byte
}

var udpObservedPeers = struct {
	mu      sync.Mutex
	entries map[string]observedUDPPeer
}{
	entries: make(map[string]observedUDPPeer),
}

var udpKeepaliveState = struct {
	mu      sync.Mutex
	lastRun map[string]time.Time
}{
	lastRun: make(map[string]time.Time),
}

var udpRecentSuccesses = struct {
	mu      sync.Mutex
	entries map[string]recentUDPSuccess
}{
	entries: make(map[string]recentUDPSuccess),
}

var udpSharedSockets = struct {
	mu      sync.Mutex
	entries map[string]*udpSharedSocket
}{
	entries: make(map[string]*udpSharedSocket),
}

type UDPServer struct {
	ListenAddr           string
	Source               ContentSource
	OnPieceServed        func(bytes int64, path string, peerID string)
	ShouldSendPieceChunk func(remote string, pieceIndex int, chunkIndex int) bool
	socket               *udpSharedSocket
}

func NewUDPServer(listenAddr string, source ContentSource) *UDPServer {
	return &UDPServer{
		ListenAddr: listenAddr,
		Source:     source,
	}
}

func (s *UDPServer) Listen(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", s.ListenAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	socket := newUDPSharedSocket(conn)
	s.socket = socket
	registerUDPSharedSocket(s.ListenAddr, socket)
	defer unregisterUDPSharedSocket(s.ListenAddr, socket)
	defer func() {
		s.socket = nil
	}()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	buffer := make([]byte, udpMaxDatagramBytes)
	for {
		n, remote, err := conn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		payload := append([]byte(nil), buffer[:n]...)
		go s.handleDatagram(socket, remote, payload)
	}
}

func (s *UDPServer) handleDatagram(socket *udpSharedSocket, remote *net.UDPAddr, payload []byte) {
	var message UDPMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		_ = socket.deliverRaw(payload)
		return
	}
	if socket.deliver(message) {
		return
	}

	switch message.Type {
	case UDPMessageTypePing:
		req, err := decodeUDPBody[UDPPing](message)
		if err != nil {
			_ = writeUDPError(socket.conn, remote, "", err)
			return
		}
		if req.PeerID != "" && req.ContentID != "" {
			RememberObservedUDPPeer(req.ContentID, req.PeerID, remote.String(), time.Now())
		}
		_ = writeUDPMessage(socket.conn, remote, UDPMessageTypePong, UDPPong{RequestID: req.RequestID})
	case UDPMessageTypeHaveRequest:
		req, err := decodeUDPBody[UDPHaveRequest](message)
		if err != nil {
			_ = writeUDPError(socket.conn, remote, "", err)
			return
		}
		ranges, err := s.Source.HaveRanges(req.ContentID)
		if err != nil {
			_ = writeUDPError(socket.conn, remote, req.RequestID, err)
			return
		}
		_ = writeUDPMessage(socket.conn, remote, UDPMessageTypeHaveResponse, UDPHaveResponse{
			RequestID:  req.RequestID,
			ContentID:  req.ContentID,
			HaveRanges: ranges,
		})
	case UDPMessageTypePieceRequest:
		req, err := decodeUDPBody[UDPPieceRequest](message)
		if err != nil {
			_ = writeUDPError(socket.conn, remote, "", err)
			return
		}
		data, err := s.Source.Piece(req.ContentID, req.PieceIndex)
		if err != nil {
			_ = writeUDPError(socket.conn, remote, req.RequestID, err)
			return
		}
		totalChunks := (len(data) + udpChunkDataBytes - 1) / udpChunkDataBytes
		for _, chunkIndex := range udpRequestedChunkIndexes(req, totalChunks) {
			start := chunkIndex * udpChunkDataBytes
			end := start + udpChunkDataBytes
			if end > len(data) {
				end = len(data)
			}
			if s.ShouldSendPieceChunk != nil && !s.ShouldSendPieceChunk(remote.String(), req.PieceIndex, chunkIndex) {
				continue
			}
			if err := writeUDPMessage(socket.conn, remote, UDPMessageTypePieceChunk, UDPPieceChunk{
				RequestID:   req.RequestID,
				ContentID:   req.ContentID,
				PieceIndex:  req.PieceIndex,
				ChunkIndex:  chunkIndex,
				TotalChunks: totalChunks,
				Data:        data[start:end],
			}); err != nil {
				return
			}
		}
		if s.OnPieceServed != nil {
			s.OnPieceServed(int64(len(data)), classifyRemotePath(remote.String()), remote.String())
		}
	}
}

type UDPClient struct {
	Addr        string
	LocalAddr   string
	Timeout     time.Duration
	ChunkWindow int
}

type UDPBurstPhase struct {
	Attempts int
	Gap      time.Duration
}

func NewUDPClient(addr string, timeout time.Duration) *UDPClient {
	return &UDPClient{
		Addr:    addr,
		Timeout: timeout,
	}
}

func (c *UDPClient) WithLocalAddr(localAddr string) *UDPClient {
	if c == nil {
		return nil
	}
	clone := *c
	clone.LocalAddr = localAddr
	return &clone
}

func (c *UDPClient) WithChunkWindow(window int) *UDPClient {
	if c == nil {
		return nil
	}
	clone := *c
	clone.ChunkWindow = window
	return &clone
}

func (c *UDPClient) Probe() error {
	return c.probeBurst("", "", 1, 0)
}

func (c *UDPClient) ProbeForPeer(contentID string, peerID string) error {
	return c.probeBurst(contentID, peerID, 1, 0)
}

func (c *UDPClient) ProbeBurstForPeer(contentID string, peerID string, attempts int, gap time.Duration) error {
	return c.probeBurst(contentID, peerID, attempts, gap)
}

func (c *UDPClient) ProbeMultiBurstForPeer(contentID string, peerID string, phases []UDPBurstPhase) error {
	return c.probeMultiBurst(contentID, peerID, phases)
}

func (c *UDPClient) probeBurst(contentID string, peerID string, attempts int, gap time.Duration) error {
	return c.probeMultiBurst(contentID, peerID, []UDPBurstPhase{{Attempts: attempts, Gap: gap}})
}

func (c *UDPClient) probeMultiBurst(contentID string, peerID string, phases []UDPBurstPhase) error {
	phases = normalizeUDPBurstPhases(phases)
	requestID, err := newUDPRequestID()
	if err != nil {
		return err
	}
	conn, remote, err := c.open()
	if err != nil {
		return err
	}
	if conn == nil {
		return c.probeWithSharedSocket(remote, requestID, contentID, peerID, phases)
	}
	defer conn.Close()

	if err := sendUDPPingPhases(conn, remote, requestID, contentID, peerID, phases); err != nil {
		return err
	}

	deadline := time.Now().Add(c.Timeout)
	buffer := make([]byte, udpMaxDatagramBytes)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return wrapUDPReadError("probe", c.Addr, err)
		}
		message, err := decodeUDPMessage(buffer[:n])
		if err != nil {
			continue
		}
		if message.Type == UDPMessageTypeError {
			body, err := decodeUDPBody[UDPError](message)
			if err != nil {
				return err
			}
			if body.RequestID == requestID {
				return errors.New(body.Message)
			}
			continue
		}
		if message.Type != UDPMessageTypePong {
			continue
		}
		body, err := decodeUDPBody[UDPPong](message)
		if err != nil {
			return err
		}
		if body.RequestID == requestID {
			return nil
		}
	}
}

func (c *UDPClient) probeWithSharedSocket(remote *net.UDPAddr, requestID string, contentID string, peerID string, phases []UDPBurstPhase) error {
	socket, ok := lookupUDPSharedSocket(c.LocalAddr)
	if !ok {
		return fmt.Errorf("shared udp socket unavailable for %s", c.LocalAddr)
	}
	msgCh, release := socket.register(requestID)
	defer release()
	if err := sendUDPPingPhases(socket.conn, remote, requestID, contentID, peerID, phases); err != nil {
		return err
	}
	deadline := time.NewTimer(c.Timeout)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			return &UDPTimeoutError{Op: "probe", Addr: c.Addr, Err: errors.New("probe deadline exceeded")}
		case message := <-msgCh:
			if message.Type == UDPMessageTypeError {
				body, err := decodeUDPBody[UDPError](message)
				if err != nil {
					return err
				}
				return errors.New(body.Message)
			}
			if message.Type != UDPMessageTypePong {
				continue
			}
			body, err := decodeUDPBody[UDPPong](message)
			if err != nil {
				return err
			}
			if body.RequestID == requestID {
				return nil
			}
		}
	}
}

func sendUDPPingPhases(conn *net.UDPConn, remote *net.UDPAddr, requestID string, contentID string, peerID string, phases []UDPBurstPhase) error {
	for phaseIndex, phase := range normalizeUDPBurstPhases(phases) {
		if phaseIndex > 0 && phase.Gap > 0 {
			time.Sleep(phase.Gap)
		}
		if err := sendUDPPingBurst(conn, remote, requestID, contentID, peerID, phase.Attempts, phase.Gap); err != nil {
			return err
		}
	}
	return nil
}

func sendUDPPingBurst(conn *net.UDPConn, remote *net.UDPAddr, requestID string, contentID string, peerID string, attempts int, gap time.Duration) error {
	for i := 0; i < attempts; i++ {
		if i > 0 && gap > 0 {
			time.Sleep(gap)
		}
		if err := writeUDPMessage(conn, remote, UDPMessageTypePing, UDPPing{
			RequestID: requestID,
			PeerID:    peerID,
			ContentID: contentID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func normalizeUDPBurstPhases(phases []UDPBurstPhase) []UDPBurstPhase {
	if len(phases) == 0 {
		return []UDPBurstPhase{{Attempts: 1}}
	}
	normalized := make([]UDPBurstPhase, 0, len(phases))
	for _, phase := range phases {
		if phase.Attempts <= 0 {
			phase.Attempts = 1
		}
		if phase.Gap < 0 {
			phase.Gap = 0
		}
		normalized = append(normalized, phase)
	}
	return normalized
}

func RememberObservedUDPPeer(contentID string, peerID string, addr string, now time.Time) {
	if contentID == "" || peerID == "" || addr == "" {
		return
	}

	udpObservedPeers.mu.Lock()
	defer udpObservedPeers.mu.Unlock()

	udpObservedPeers.entries[contentID+"|"+peerID] = observedUDPPeer{
		Addr:   addr,
		SeenAt: now,
	}
}

func ObservedUDPPeerAddr(contentID string, peerID string, maxAge time.Duration, now time.Time) (string, bool) {
	if contentID == "" || peerID == "" {
		return "", false
	}

	udpObservedPeers.mu.Lock()
	defer udpObservedPeers.mu.Unlock()

	key := contentID + "|" + peerID
	entry, ok := udpObservedPeers.entries[key]
	if !ok {
		return "", false
	}
	if maxAge > 0 && now.Sub(entry.SeenAt) > maxAge {
		delete(udpObservedPeers.entries, key)
		return "", false
	}
	return entry.Addr, true
}

func ObservedUDPPeer(contentID string, peerID string, maxAge time.Duration, now time.Time) (string, time.Time, bool) {
	if contentID == "" || peerID == "" {
		return "", time.Time{}, false
	}

	udpObservedPeers.mu.Lock()
	defer udpObservedPeers.mu.Unlock()

	key := contentID + "|" + peerID
	entry, ok := udpObservedPeers.entries[key]
	if !ok {
		return "", time.Time{}, false
	}
	if maxAge > 0 && now.Sub(entry.SeenAt) > maxAge {
		delete(udpObservedPeers.entries, key)
		return "", time.Time{}, false
	}
	return entry.Addr, entry.SeenAt, true
}

func ShouldKeepAliveObservedUDPPeer(localAddr string, contentID string, peerID string, remoteAddr string, interval time.Duration, now time.Time) bool {
	return shouldKeepAliveUDPPath(localAddr, contentID+"|"+peerID+"|"+remoteAddr, interval, now)
}

func ShouldKeepAliveRecentUDPSuccess(localAddr string, contentID string, remoteAddr string, interval time.Duration, now time.Time) bool {
	return shouldKeepAliveUDPPath(localAddr, contentID+"|recent-success|"+remoteAddr, interval, now)
}

func shouldKeepAliveUDPPath(localAddr string, pathKey string, interval time.Duration, now time.Time) bool {
	if strings.TrimSpace(localAddr) == "" || strings.TrimSpace(pathKey) == "" || interval <= 0 {
		return false
	}
	if _, ok := lookupUDPSharedSocket(localAddr); !ok {
		return false
	}
	key := localAddr + "|" + pathKey
	udpKeepaliveState.mu.Lock()
	defer udpKeepaliveState.mu.Unlock()
	lastRun, ok := udpKeepaliveState.lastRun[key]
	if ok && now.Sub(lastRun) < interval {
		return false
	}
	udpKeepaliveState.lastRun[key] = now
	return true
}

func RememberRecentUDPSuccess(contentID string, addr string, now time.Time) {
	if strings.TrimSpace(contentID) == "" || strings.TrimSpace(addr) == "" {
		return
	}
	udpRecentSuccesses.mu.Lock()
	defer udpRecentSuccesses.mu.Unlock()
	udpRecentSuccesses.entries[contentID+"|"+addr] = recentUDPSuccess{
		Addr:   addr,
		SeenAt: now,
	}
}

func RecentUDPSuccessAddrs(contentID string, maxAge time.Duration, now time.Time) []string {
	if strings.TrimSpace(contentID) == "" {
		return nil
	}
	udpRecentSuccesses.mu.Lock()
	defer udpRecentSuccesses.mu.Unlock()
	prefix := contentID + "|"
	addrs := make([]string, 0)
	for key, entry := range udpRecentSuccesses.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if maxAge > 0 && now.Sub(entry.SeenAt) > maxAge {
			delete(udpRecentSuccesses.entries, key)
			continue
		}
		addrs = append(addrs, entry.Addr)
	}
	sort.Strings(addrs)
	return addrs
}

func (c *UDPClient) FetchHave(contentID string) ([]core.HaveRange, error) {
	requestID, err := newUDPRequestID()
	if err != nil {
		return nil, err
	}
	conn, remote, err := c.open()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return c.fetchHaveWithSharedSocket(remote, requestID, contentID)
	}
	defer conn.Close()

	if err := writeUDPMessage(conn, remote, UDPMessageTypeHaveRequest, UDPHaveRequest{
		RequestID: requestID,
		ContentID: contentID,
	}); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(c.Timeout)
	buffer := make([]byte, udpMaxDatagramBytes)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return nil, wrapUDPReadError("fetch-have", c.Addr, err)
		}
		message, err := decodeUDPMessage(buffer[:n])
		if err != nil {
			continue
		}
		if message.Type == UDPMessageTypeError {
			body, err := decodeUDPBody[UDPError](message)
			if err != nil {
				return nil, err
			}
			if body.RequestID == requestID {
				return nil, errors.New(body.Message)
			}
			continue
		}
		if message.Type != UDPMessageTypeHaveResponse {
			continue
		}
		body, err := decodeUDPBody[UDPHaveResponse](message)
		if err != nil {
			return nil, err
		}
		if body.RequestID != requestID {
			continue
		}
		return body.HaveRanges, nil
	}
}

func (c *UDPClient) fetchHaveWithSharedSocket(remote *net.UDPAddr, requestID string, contentID string) ([]core.HaveRange, error) {
	socket, ok := lookupUDPSharedSocket(c.LocalAddr)
	if !ok {
		return nil, fmt.Errorf("shared udp socket unavailable for %s", c.LocalAddr)
	}
	msgCh, release := socket.register(requestID)
	defer release()
	if err := writeUDPMessage(socket.conn, remote, UDPMessageTypeHaveRequest, UDPHaveRequest{
		RequestID: requestID,
		ContentID: contentID,
	}); err != nil {
		return nil, err
	}
	deadline := time.NewTimer(c.Timeout)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			return nil, &UDPTimeoutError{Op: "fetch-have", Addr: c.Addr, Err: errors.New("have deadline exceeded")}
		case message := <-msgCh:
			if message.Type == UDPMessageTypeError {
				body, err := decodeUDPBody[UDPError](message)
				if err != nil {
					return nil, err
				}
				return nil, errors.New(body.Message)
			}
			if message.Type != UDPMessageTypeHaveResponse {
				continue
			}
			body, err := decodeUDPBody[UDPHaveResponse](message)
			if err != nil {
				return nil, err
			}
			if body.RequestID == requestID {
				return body.HaveRanges, nil
			}
		}
	}
}

func (c *UDPClient) FetchPiece(contentID string, pieceIndex int) ([]byte, error) {
	requestID, err := newUDPRequestID()
	if err != nil {
		return nil, err
	}
	conn, remote, err := c.open()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return c.fetchPieceWithSharedSocket(remote, requestID, contentID, pieceIndex)
	}
	defer conn.Close()

	if err := writeUDPPieceRequest(conn, remote, requestID, contentID, pieceIndex, []int{0}); err != nil {
		return nil, err
	}

	buffer := make([]byte, udpMaxDatagramBytes)
	return c.collectPieceChunksWithConn(conn, remote, requestID, contentID, pieceIndex, buffer)
}

func (c *UDPClient) open() (*net.UDPConn, *net.UDPAddr, error) {
	remote, err := net.ResolveUDPAddr("udp", c.Addr)
	if err != nil {
		return nil, nil, err
	}
	if c.LocalAddr != "" {
		if _, ok := lookupUDPSharedSocket(c.LocalAddr); ok {
			return nil, remote, nil
		}
	}
	var local *net.UDPAddr
	if c.LocalAddr != "" {
		local, err = net.ResolveUDPAddr("udp", c.LocalAddr)
		if err != nil {
			return nil, nil, err
		}
	}
	conn, err := net.ListenUDP("udp", local)
	if err != nil {
		return nil, nil, err
	}
	return conn, remote, nil
}

func (c *UDPClient) fetchPieceWithSharedSocket(remote *net.UDPAddr, requestID string, contentID string, pieceIndex int) ([]byte, error) {
	socket, ok := lookupUDPSharedSocket(c.LocalAddr)
	if !ok {
		return nil, fmt.Errorf("shared udp socket unavailable for %s", c.LocalAddr)
	}
	msgCh, release := socket.register(requestID)
	defer release()
	if err := writeUDPPieceRequest(socket.conn, remote, requestID, contentID, pieceIndex, []int{0}); err != nil {
		return nil, err
	}
	return c.collectPieceChunksWithSharedSocket(socket.conn, remote, msgCh, requestID, contentID, pieceIndex)
}

func (c *UDPClient) collectPieceChunksWithConn(conn *net.UDPConn, remote *net.UDPAddr, requestID string, contentID string, pieceIndex int, buffer []byte) ([]byte, error) {
	chunks := make(map[int][]byte)
	totalChunks := -1
	window := c.chunkWindow()
	for round := 0; round < 6; round++ {
		deadline := time.Now().Add(c.Timeout)
		for {
			if totalChunks >= 0 && len(chunks) == totalChunks {
				return joinUDPChunks(chunks, totalChunks), nil
			}
			if time.Now().After(deadline) {
				break
			}
			if err := conn.SetReadDeadline(deadline); err != nil {
				return nil, err
			}
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				if IsUDPTimeout(err) {
					break
				}
				return nil, wrapUDPReadError(fmt.Sprintf("fetch-piece-%d", pieceIndex), c.Addr, err)
			}
			message, err := decodeUDPMessage(buffer[:n])
			if err != nil {
				continue
			}
			if message.Type == UDPMessageTypeError {
				body, err := decodeUDPBody[UDPError](message)
				if err != nil {
					return nil, err
				}
				return nil, errors.New(body.Message)
			}
			if !collectUDPPieceChunk(message, requestID, contentID, pieceIndex, chunks, &totalChunks) {
				continue
			}
		}
		if totalChunks >= 0 && len(chunks) == totalChunks {
			return joinUDPChunks(chunks, totalChunks), nil
		}
		if totalChunks <= 0 {
			if err := writeUDPPieceRequest(conn, remote, requestID, contentID, pieceIndex, []int{0}); err != nil {
				return nil, err
			}
			continue
		}
		nextBatch := nextUDPPieceChunkBatch(chunks, totalChunks, window)
		if len(nextBatch) == 0 {
			return joinUDPChunks(chunks, totalChunks), nil
		}
		if err := writeUDPPieceRequest(conn, remote, requestID, contentID, pieceIndex, nextBatch); err != nil {
			return nil, err
		}
	}
	return nil, &UDPTimeoutError{
		Op:   fmt.Sprintf("fetch-piece-%d", pieceIndex),
		Addr: c.Addr,
		Err:  errors.New("piece deadline exceeded"),
	}
}

func (c *UDPClient) collectPieceChunksWithSharedSocket(conn *net.UDPConn, remote *net.UDPAddr, msgCh <-chan UDPMessage, requestID string, contentID string, pieceIndex int) ([]byte, error) {
	chunks := make(map[int][]byte)
	totalChunks := -1
	window := c.chunkWindow()
	for round := 0; round < 6; round++ {
		deadline := time.NewTimer(c.Timeout)
		timedOut := false
		for !timedOut {
			if totalChunks >= 0 && len(chunks) == totalChunks {
				deadline.Stop()
				return joinUDPChunks(chunks, totalChunks), nil
			}
			select {
			case <-deadline.C:
				timedOut = true
			case message := <-msgCh:
				if message.Type == UDPMessageTypeError {
					body, err := decodeUDPBody[UDPError](message)
					if err != nil {
						deadline.Stop()
						return nil, err
					}
					deadline.Stop()
					return nil, errors.New(body.Message)
				}
				collectUDPPieceChunk(message, requestID, contentID, pieceIndex, chunks, &totalChunks)
			}
		}
		if totalChunks >= 0 && len(chunks) == totalChunks {
			return joinUDPChunks(chunks, totalChunks), nil
		}
		if totalChunks <= 0 {
			if err := writeUDPPieceRequest(conn, remote, requestID, contentID, pieceIndex, []int{0}); err != nil {
				return nil, err
			}
			continue
		}
		nextBatch := nextUDPPieceChunkBatch(chunks, totalChunks, window)
		if len(nextBatch) == 0 {
			return joinUDPChunks(chunks, totalChunks), nil
		}
		if err := writeUDPPieceRequest(conn, remote, requestID, contentID, pieceIndex, nextBatch); err != nil {
			return nil, err
		}
	}
	return nil, &UDPTimeoutError{
		Op:   fmt.Sprintf("fetch-piece-%d", pieceIndex),
		Addr: c.Addr,
		Err:  errors.New("piece deadline exceeded"),
	}
}

func writeUDPPieceRequest(conn *net.UDPConn, remote *net.UDPAddr, requestID string, contentID string, pieceIndex int, chunkIndexes []int) error {
	return writeUDPMessage(conn, remote, UDPMessageTypePieceRequest, UDPPieceRequest{
		RequestID:    requestID,
		ContentID:    contentID,
		PieceIndex:   pieceIndex,
		ChunkIndexes: append([]int(nil), chunkIndexes...),
	})
}

func collectUDPPieceChunk(message UDPMessage, requestID string, contentID string, pieceIndex int, chunks map[int][]byte, totalChunks *int) bool {
	if message.Type != UDPMessageTypePieceChunk {
		return false
	}
	chunk, err := decodeUDPBody[UDPPieceChunk](message)
	if err != nil {
		return false
	}
	if chunk.RequestID != requestID || chunk.ContentID != contentID || chunk.PieceIndex != pieceIndex {
		return false
	}
	if chunk.TotalChunks <= 0 || chunk.ChunkIndex < 0 || chunk.ChunkIndex >= chunk.TotalChunks {
		return false
	}
	if *totalChunks == -1 {
		*totalChunks = chunk.TotalChunks
	}
	if chunk.TotalChunks != *totalChunks {
		return false
	}
	chunks[chunk.ChunkIndex] = chunk.Data
	return true
}

func missingUDPPieceChunks(chunks map[int][]byte, totalChunks int) []int {
	if totalChunks <= 0 {
		return nil
	}
	missing := make([]int, 0, totalChunks-len(chunks))
	for i := 0; i < totalChunks; i++ {
		if _, ok := chunks[i]; ok {
			continue
		}
		missing = append(missing, i)
	}
	return missing
}

func nextUDPPieceChunkBatch(chunks map[int][]byte, totalChunks int, window int) []int {
	missing := missingUDPPieceChunks(chunks, totalChunks)
	if len(missing) == 0 {
		return nil
	}
	if window <= 0 || len(missing) <= window {
		return missing
	}
	return append([]int(nil), missing[:window]...)
}

func (c *UDPClient) chunkWindow() int {
	if c != nil && c.ChunkWindow > 0 {
		return c.ChunkWindow
	}
	return udpPieceChunkWindow
}

func udpRequestedChunkIndexes(req UDPPieceRequest, totalChunks int) []int {
	if totalChunks <= 0 {
		return nil
	}
	if len(req.ChunkIndexes) == 0 {
		indexes := make([]int, 0, totalChunks)
		for i := 0; i < totalChunks; i++ {
			indexes = append(indexes, i)
		}
		return indexes
	}
	seen := make(map[int]bool, len(req.ChunkIndexes))
	indexes := make([]int, 0, len(req.ChunkIndexes))
	for _, chunkIndex := range req.ChunkIndexes {
		if chunkIndex < 0 || chunkIndex >= totalChunks || seen[chunkIndex] {
			continue
		}
		seen[chunkIndex] = true
		indexes = append(indexes, chunkIndex)
	}
	sort.Ints(indexes)
	return indexes
}

func newUDPSharedSocket(conn *net.UDPConn) *udpSharedSocket {
	return &udpSharedSocket{
		conn:    conn,
		wait:    make(map[string]udpPendingRequest),
		rawWait: make(map[string]chan []byte),
	}
}

func (s *udpSharedSocket) register(requestID string) (<-chan UDPMessage, func()) {
	ch := make(chan UDPMessage, 32)
	s.mu.Lock()
	s.wait[requestID] = udpPendingRequest{ch: ch}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.wait, requestID)
		s.mu.Unlock()
	}
}

func (s *udpSharedSocket) registerRaw(key string) (<-chan []byte, func()) {
	ch := make(chan []byte, 8)
	s.mu.Lock()
	s.rawWait[key] = ch
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.rawWait, key)
		s.mu.Unlock()
	}
}

func (s *udpSharedSocket) deliver(message UDPMessage) bool {
	requestID := udpMessageRequestID(message)
	if requestID == "" {
		return false
	}
	s.mu.Lock()
	pending, ok := s.wait[requestID]
	s.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case pending.ch <- message:
	default:
	}
	return true
}

func (s *udpSharedSocket) deliverRaw(payload []byte) bool {
	key, ok := stunTransactionKey(payload)
	if !ok {
		return false
	}
	s.mu.Lock()
	ch, ok := s.rawWait[key]
	s.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- append([]byte(nil), payload...):
	default:
	}
	return true
}

func udpMessageRequestID(message UDPMessage) string {
	switch message.Type {
	case UDPMessageTypePong:
		body, err := decodeUDPBody[UDPPong](message)
		if err != nil {
			return ""
		}
		return body.RequestID
	case UDPMessageTypeHaveResponse:
		body, err := decodeUDPBody[UDPHaveResponse](message)
		if err != nil {
			return ""
		}
		return body.RequestID
	case UDPMessageTypePieceChunk:
		body, err := decodeUDPBody[UDPPieceChunk](message)
		if err != nil {
			return ""
		}
		return body.RequestID
	case UDPMessageTypeError:
		body, err := decodeUDPBody[UDPError](message)
		if err != nil {
			return ""
		}
		return body.RequestID
	default:
		return ""
	}
}

func registerUDPSharedSocket(listenAddr string, socket *udpSharedSocket) {
	if listenAddr == "" || socket == nil {
		return
	}
	udpSharedSockets.mu.Lock()
	defer udpSharedSockets.mu.Unlock()
	udpSharedSockets.entries[listenAddr] = socket
}

func unregisterUDPSharedSocket(listenAddr string, socket *udpSharedSocket) {
	if listenAddr == "" || socket == nil {
		return
	}
	udpSharedSockets.mu.Lock()
	defer udpSharedSockets.mu.Unlock()
	if udpSharedSockets.entries[listenAddr] == socket {
		delete(udpSharedSockets.entries, listenAddr)
	}
}

func lookupUDPSharedSocket(listenAddr string) (*udpSharedSocket, bool) {
	if listenAddr == "" {
		return nil, false
	}
	udpSharedSockets.mu.Lock()
	defer udpSharedSockets.mu.Unlock()
	socket, ok := udpSharedSockets.entries[listenAddr]
	return socket, ok
}

func writeUDPMessage(conn *net.UDPConn, remote *net.UDPAddr, messageType string, body any) error {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	messageBytes, err := json.Marshal(UDPMessage{Type: messageType, Body: bodyBytes})
	if err != nil {
		return err
	}
	_, err = conn.WriteToUDP(messageBytes, remote)
	return err
}

func writeUDPError(conn *net.UDPConn, remote *net.UDPAddr, requestID string, err error) error {
	return writeUDPMessage(conn, remote, UDPMessageTypeError, UDPError{
		RequestID: requestID,
		Message:   err.Error(),
	})
}

func decodeUDPMessage(data []byte) (UDPMessage, error) {
	var message UDPMessage
	err := json.Unmarshal(data, &message)
	return message, err
}

func decodeUDPBody[T any](message UDPMessage) (T, error) {
	var body T
	if len(message.Body) == 0 {
		return body, fmt.Errorf("udp message %s has empty body", message.Type)
	}
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return body, err
	}
	return body, nil
}

func newUDPRequestID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func joinUDPChunks(chunks map[int][]byte, totalChunks int) []byte {
	indexes := make([]int, 0, len(chunks))
	for index := range chunks {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)

	var data []byte
	for _, index := range indexes {
		if index >= totalChunks {
			continue
		}
		data = append(data, chunks[index]...)
	}
	return data
}

func IsUDPTimeout(err error) bool {
	if err == nil {
		return false
	}
	var udpTimeout *UDPTimeoutError
	if errors.As(err, &udpTimeout) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "timed out")
}

func wrapUDPReadError(op string, addr string, err error) error {
	if err == nil {
		return nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &UDPTimeoutError{
			Op:   op,
			Addr: addr,
			Err:  err,
		}
	}
	return err
}
