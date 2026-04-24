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
)

type UDPMessage struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body,omitempty"`
}

type UDPPing struct {
	RequestID string `json:"requestId"`
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
	RequestID  string `json:"requestId"`
	ContentID  string `json:"contentId"`
	PieceIndex int    `json:"pieceIndex"`
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

type UDPServer struct {
	ListenAddr    string
	Source        ContentSource
	OnPieceServed func(bytes int64, path string, peerID string)
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
		go s.handleDatagram(conn, remote, payload)
	}
}

func (s *UDPServer) handleDatagram(conn *net.UDPConn, remote *net.UDPAddr, payload []byte) {
	var message UDPMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return
	}

	switch message.Type {
	case UDPMessageTypePing:
		req, err := decodeUDPBody[UDPPing](message)
		if err != nil {
			_ = writeUDPError(conn, remote, "", err)
			return
		}
		_ = writeUDPMessage(conn, remote, UDPMessageTypePong, UDPPong{RequestID: req.RequestID})
	case UDPMessageTypeHaveRequest:
		req, err := decodeUDPBody[UDPHaveRequest](message)
		if err != nil {
			_ = writeUDPError(conn, remote, "", err)
			return
		}
		ranges, err := s.Source.HaveRanges(req.ContentID)
		if err != nil {
			_ = writeUDPError(conn, remote, req.RequestID, err)
			return
		}
		_ = writeUDPMessage(conn, remote, UDPMessageTypeHaveResponse, UDPHaveResponse{
			RequestID:  req.RequestID,
			ContentID:  req.ContentID,
			HaveRanges: ranges,
		})
	case UDPMessageTypePieceRequest:
		req, err := decodeUDPBody[UDPPieceRequest](message)
		if err != nil {
			_ = writeUDPError(conn, remote, "", err)
			return
		}
		data, err := s.Source.Piece(req.ContentID, req.PieceIndex)
		if err != nil {
			_ = writeUDPError(conn, remote, req.RequestID, err)
			return
		}
		totalChunks := (len(data) + udpChunkDataBytes - 1) / udpChunkDataBytes
		for chunkIndex := 0; chunkIndex < totalChunks; chunkIndex++ {
			start := chunkIndex * udpChunkDataBytes
			end := start + udpChunkDataBytes
			if end > len(data) {
				end = len(data)
			}
			if err := writeUDPMessage(conn, remote, UDPMessageTypePieceChunk, UDPPieceChunk{
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
	Addr    string
	Timeout time.Duration
}

func NewUDPClient(addr string, timeout time.Duration) *UDPClient {
	return &UDPClient{
		Addr:    addr,
		Timeout: timeout,
	}
}

func (c *UDPClient) Probe() error {
	requestID, err := newUDPRequestID()
	if err != nil {
		return err
	}
	conn, remote, err := c.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeUDPMessage(conn, remote, UDPMessageTypePing, UDPPing{RequestID: requestID}); err != nil {
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
			return err
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

func (c *UDPClient) FetchHave(contentID string) ([]core.HaveRange, error) {
	requestID, err := newUDPRequestID()
	if err != nil {
		return nil, err
	}
	conn, remote, err := c.open()
	if err != nil {
		return nil, err
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
			return nil, err
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

func (c *UDPClient) FetchPiece(contentID string, pieceIndex int) ([]byte, error) {
	requestID, err := newUDPRequestID()
	if err != nil {
		return nil, err
	}
	conn, remote, err := c.open()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := writeUDPMessage(conn, remote, UDPMessageTypePieceRequest, UDPPieceRequest{
		RequestID:  requestID,
		ContentID:  contentID,
		PieceIndex: pieceIndex,
	}); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(c.Timeout)
	chunks := make(map[int][]byte)
	totalChunks := -1
	buffer := make([]byte, udpMaxDatagramBytes)

	for {
		if totalChunks >= 0 && len(chunks) == totalChunks {
			return joinUDPChunks(chunks, totalChunks), nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("udp piece %d timed out", pieceIndex)
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return nil, err
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
		if message.Type != UDPMessageTypePieceChunk {
			continue
		}
		chunk, err := decodeUDPBody[UDPPieceChunk](message)
		if err != nil {
			return nil, err
		}
		if chunk.RequestID != requestID || chunk.ContentID != contentID || chunk.PieceIndex != pieceIndex {
			continue
		}
		if chunk.TotalChunks <= 0 || chunk.ChunkIndex < 0 || chunk.ChunkIndex >= chunk.TotalChunks {
			continue
		}
		if totalChunks == -1 {
			totalChunks = chunk.TotalChunks
		}
		if chunk.TotalChunks != totalChunks {
			continue
		}
		chunks[chunk.ChunkIndex] = chunk.Data
	}
}

func (c *UDPClient) open() (*net.UDPConn, *net.UDPAddr, error) {
	remote, err := net.ResolveUDPAddr("udp", c.Addr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, nil, err
	}
	return conn, remote, nil
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
