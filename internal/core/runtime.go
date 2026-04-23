package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type RuntimeStats struct {
	mu   sync.Mutex
	path string
	data RuntimeData
}

type RuntimeData struct {
	DownloadBytes int64     `json:"downloadBytes"`
	UploadBytes   int64     `json:"uploadBytes"`
	DownloadRate  int64     `json:"downloadRate"`
	UploadRate    int64     `json:"uploadRate"`
	Peers         int       `json:"peers"`
	PathStats     PathStats `json:"pathStats"`
}

func OpenRuntimeStats(root string) (*RuntimeStats, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}

	stats := &RuntimeStats{
		path: filepath.Join(root, "runtime.json"),
	}
	if err := stats.load(); err != nil {
		return nil, err
	}
	return stats, nil
}

func (r *RuntimeStats) Snapshot() RuntimeData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.data
}

func (r *RuntimeStats) SetPeers(peers int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.Peers = peers
	return r.saveLocked()
}

func (r *RuntimeStats) RecordDownload(bytes int64, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.DownloadBytes += bytes
	r.data.DownloadRate = bytes
	applyPathBytes(&r.data.PathStats, path, bytes)
	return r.saveLocked()
}

func (r *RuntimeStats) RecordUpload(bytes int64, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.UploadBytes += bytes
	r.data.UploadRate = bytes
	applyPathBytes(&r.data.PathStats, path, bytes)
	return r.saveLocked()
}

func (r *RuntimeStats) load() error {
	data, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &r.data)
}

func (r *RuntimeStats) saveLocked() error {
	data, err := json.MarshalIndent(r.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.path, data, 0o644)
}

func applyPathBytes(stats *PathStats, path string, bytes int64) {
	switch path {
	case "lan":
		stats.LANBytes += bytes
	case "relay":
		stats.RelayBytes += bytes
	default:
		stats.DirectBytes += bytes
	}
}
