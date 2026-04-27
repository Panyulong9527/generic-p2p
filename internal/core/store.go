package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type PieceStore struct {
	root         string
	manifest     *ContentManifest
	mu           sync.Mutex
	metaPath     string
	completedSet map[int]bool
	runtime      *RuntimeStats
}

type StoreStatus struct {
	ContentID       string               `json:"contentId"`
	State           string               `json:"state"`
	ProgressValue   float64              `json:"progress"`
	CompletedPieces int                  `json:"completedPieces"`
	TotalPieces     int                  `json:"totalPieces"`
	CompletedRanges []HaveRange          `json:"completedRanges"`
	DownloadBytes   int64                `json:"downloadBytes"`
	UploadBytes     int64                `json:"uploadBytes"`
	DownloadRate    int64                `json:"downloadRate"`
	UploadRate      int64                `json:"uploadRate"`
	Peers           int                  `json:"peers"`
	PathStats       PathStats            `json:"pathStats"`
	PeerStats       map[string]PeerStats `json:"peerStats,omitempty"`
	ActiveDownloads []ActiveDownload     `json:"activeDownloads,omitempty"`
	RecentDecisions []SelectionDecision  `json:"recentDecisions,omitempty"`
}

func (s StoreStatus) Progress() float64 {
	return s.ProgressValue
}

type PathStats struct {
	LANBytes    int64 `json:"lanBytes"`
	DirectBytes int64 `json:"directBytes"`
	RelayBytes  int64 `json:"relayBytes"`
}

type pieceStoreMeta struct {
	CompletedPieces []int `json:"completedPieces"`
}

func OpenPieceStore(root string, manifest *ContentManifest) (*PieceStore, error) {
	if err := ValidateManifest(manifest); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "pieces"), 0o755); err != nil {
		return nil, err
	}

	store := &PieceStore{
		root:         root,
		manifest:     manifest,
		metaPath:     filepath.Join(root, "store.json"),
		completedSet: make(map[int]bool),
	}
	if err := store.loadMeta(); err != nil {
		return nil, err
	}
	runtime, err := OpenRuntimeStats(root)
	if err != nil {
		return nil, err
	}
	store.runtime = runtime
	return store, nil
}

func (s *PieceStore) Close() error {
	return nil
}

func (s *PieceStore) Manifest() *ContentManifest {
	return s.manifest
}

func (s *PieceStore) RuntimeStats() *RuntimeStats {
	return s.runtime
}

func (s *PieceStore) PutPiece(index int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	piece, err := s.pieceByIndex(index)
	if err != nil {
		return err
	}
	if err := VerifyPieceData(piece, data); err != nil {
		return err
	}
	if err := os.WriteFile(s.piecePath(index), data, 0o644); err != nil {
		return err
	}
	s.completedSet[index] = true
	return s.saveMetaLocked()
}

func (s *PieceStore) GetPiece(index int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.completedSet[index] {
		return nil, fmt.Errorf("piece %d not completed", index)
	}
	return os.ReadFile(s.piecePath(index))
}

func (s *PieceStore) HasPiece(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completedSet[index]
}

func (s *PieceStore) CompletedPieceMap() map[int]bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[int]bool, len(s.completedSet))
	for index := range s.completedSet {
		result[index] = true
	}
	return result
}

func (s *PieceStore) Status() StoreStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	completedIndexes := make([]int, 0, len(s.completedSet))
	var downloadBytes int64
	for index := range s.completedSet {
		completedIndexes = append(completedIndexes, index)
		downloadBytes += s.manifest.Pieces[index].Length
	}

	state := "downloading"
	if len(completedIndexes) == len(s.manifest.Pieces) && len(s.manifest.Pieces) > 0 {
		state = "completed"
	}

	return StoreStatus{
		ContentID:       s.manifest.ContentID,
		State:           state,
		ProgressValue:   progress(len(completedIndexes), len(s.manifest.Pieces)),
		CompletedPieces: len(completedIndexes),
		TotalPieces:     len(s.manifest.Pieces),
		CompletedRanges: CompressPieceIndexes(completedIndexes),
		DownloadBytes:   downloadBytes,
		UploadBytes:     runtimeSnapshot(s.runtime).UploadBytes,
		DownloadRate:    runtimeSnapshot(s.runtime).DownloadRate,
		UploadRate:      runtimeSnapshot(s.runtime).UploadRate,
		Peers:           runtimeSnapshot(s.runtime).Peers,
		PathStats:       mergePathStats(runtimeSnapshot(s.runtime).PathStats, downloadBytes),
		PeerStats:       runtimeSnapshot(s.runtime).PeerStats,
		ActiveDownloads: runtimeSnapshot(s.runtime).ActiveDownloads,
		RecentDecisions: runtimeSnapshot(s.runtime).RecentDecisions,
	}
}

func progress(completedPieces int, totalPieces int) float64 {
	if totalPieces == 0 {
		return 0
	}
	return float64(completedPieces) / float64(totalPieces)
}

func (s *PieceStore) CompletedRanges() []HaveRange {
	return s.Status().CompletedRanges
}

func (s *PieceStore) AssembleTo(outPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, piece := range s.manifest.Pieces {
		if !s.completedSet[piece.Index] {
			return fmt.Errorf("piece %d is missing", piece.Index)
		}
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	for _, piece := range s.manifest.Pieces {
		data, err := os.ReadFile(s.piecePath(piece.Index))
		if err != nil {
			return err
		}
		if err := VerifyPieceData(piece, data); err != nil {
			return err
		}
		if _, err := outFile.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *PieceStore) loadMeta() error {
	data, err := os.ReadFile(s.metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var meta pieceStoreMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	for _, index := range meta.CompletedPieces {
		piece, err := s.pieceByIndex(index)
		if err != nil {
			continue
		}
		pieceData, err := os.ReadFile(s.piecePath(index))
		if err != nil {
			continue
		}
		if VerifyPieceData(piece, pieceData) == nil {
			s.completedSet[index] = true
		}
	}
	return nil
}

func (s *PieceStore) saveMetaLocked() error {
	indexes := make([]int, 0, len(s.completedSet))
	for index := range s.completedSet {
		indexes = append(indexes, index)
	}
	meta := pieceStoreMeta{CompletedPieces: indexes}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.metaPath, data, 0o644)
}

func (s *PieceStore) pieceByIndex(index int) (PieceInfo, error) {
	if index < 0 || index >= len(s.manifest.Pieces) {
		return PieceInfo{}, fmt.Errorf("piece index out of range: %d", index)
	}
	return s.manifest.Pieces[index], nil
}

func (s *PieceStore) piecePath(index int) string {
	return filepath.Join(s.root, "pieces", fmt.Sprintf("%06d.piece", index))
}

func runtimeSnapshot(runtime *RuntimeStats) RuntimeData {
	if runtime == nil {
		return RuntimeData{}
	}
	return runtime.Snapshot()
}

func mergePathStats(runtime PathStats, downloadBytes int64) PathStats {
	if runtime.LANBytes == 0 && runtime.DirectBytes == 0 && runtime.RelayBytes == 0 && downloadBytes > 0 {
		runtime.DirectBytes = downloadBytes
	}
	return runtime
}
