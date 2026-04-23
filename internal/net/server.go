package net

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"generic-p2p/internal/core"
)

type ContentSource interface {
	Manifest(contentID string) (*core.ContentManifest, error)
	HaveRanges(contentID string) ([]core.HaveRange, error)
	Piece(contentID string, pieceIndex int) ([]byte, error)
}

type StaticContentSource struct {
	ManifestFile *core.ContentManifest
	FilePath     string
}

type StoreContentSource struct {
	Store *core.PieceStore
}

func (s StaticContentSource) Manifest(contentID string) (*core.ContentManifest, error) {
	if s.ManifestFile == nil {
		return nil, fmt.Errorf("manifest is nil")
	}
	if s.ManifestFile.ContentID != contentID {
		return nil, fmt.Errorf("content not found: %s", contentID)
	}
	return s.ManifestFile, nil
}

func (s StaticContentSource) Piece(contentID string, pieceIndex int) ([]byte, error) {
	manifest, err := s.Manifest(contentID)
	if err != nil {
		return nil, err
	}
	return core.ReadPieceFromFile(s.FilePath, manifest, pieceIndex)
}

func (s StaticContentSource) HaveRanges(contentID string) ([]core.HaveRange, error) {
	manifest, err := s.Manifest(contentID)
	if err != nil {
		return nil, err
	}
	if len(manifest.Pieces) == 0 {
		return nil, nil
	}
	return []core.HaveRange{
		{
			Start: 0,
			End:   len(manifest.Pieces) - 1,
		},
	}, nil
}

func (s StoreContentSource) Manifest(contentID string) (*core.ContentManifest, error) {
	if s.Store == nil || s.Store.Manifest() == nil {
		return nil, fmt.Errorf("store manifest is nil")
	}
	if s.Store.Manifest().ContentID != contentID {
		return nil, fmt.Errorf("content not found: %s", contentID)
	}
	return s.Store.Manifest(), nil
}

func (s StoreContentSource) HaveRanges(contentID string) ([]core.HaveRange, error) {
	if _, err := s.Manifest(contentID); err != nil {
		return nil, err
	}
	return s.Store.CompletedRanges(), nil
}

func (s StoreContentSource) Piece(contentID string, pieceIndex int) ([]byte, error) {
	if _, err := s.Manifest(contentID); err != nil {
		return nil, err
	}
	return s.Store.GetPiece(pieceIndex)
}

type Server struct {
	ListenAddr    string
	Source        ContentSource
	OnPieceServed func(bytes int64, path string)
}

func NewServer(listenAddr string, source ContentSource) *Server {
	return &Server{
		ListenAddr: listenAddr,
		Source:     source,
	}
}

func (s *Server) Listen(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	message, err := ReadMessage(conn)
	if err != nil {
		_ = writeError(conn, err)
		return
	}

	switch message.Type {
	case MessageTypeManifestRequest:
		req, err := DecodeBody[ManifestRequest](message)
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		manifest, err := s.Source.Manifest(req.ContentID)
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		response, err := EncodeMessage(MessageTypeManifestResponse, ManifestResponse{Manifest: manifest})
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		_ = WriteMessage(conn, response)
	case MessageTypeHaveRequest:
		req, err := DecodeBody[HaveRequest](message)
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		ranges, err := s.Source.HaveRanges(req.ContentID)
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		response, err := EncodeMessage(MessageTypeHaveResponse, HaveResponse{
			ContentID:  req.ContentID,
			HaveRanges: ranges,
		})
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		_ = WriteMessage(conn, response)
	case MessageTypePieceRequest:
		req, err := DecodeBody[PieceRequest](message)
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		data, err := s.Source.Piece(req.ContentID, req.PieceIndex)
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		response, err := EncodeMessage(MessageTypePieceData, PieceData{
			ContentID:  req.ContentID,
			PieceIndex: req.PieceIndex,
			Data:       data,
		})
		if err != nil {
			_ = writeError(conn, err)
			return
		}
		_ = WriteMessage(conn, response)
		if s.OnPieceServed != nil {
			s.OnPieceServed(int64(len(data)), classifyRemotePath(conn.RemoteAddr().String()))
		}
	default:
		_ = writeError(conn, fmt.Errorf("unsupported message type: %s", message.Type))
	}
}

func classifyRemotePath(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "127.0.0.1" || host == "::1" || strings.HasPrefix(host, "192.168.") || strings.HasPrefix(host, "10.") || strings.HasPrefix(host, "172.") {
		return "lan"
	}
	return "direct"
}

func writeError(conn net.Conn, err error) error {
	message, encodeErr := EncodeMessage(MessageTypeError, ErrorMessage{Message: err.Error()})
	if encodeErr != nil {
		return encodeErr
	}
	return WriteMessage(conn, message)
}
