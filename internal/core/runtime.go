package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type RuntimeStats struct {
	mu   sync.Mutex
	path string
	data RuntimeData
	now  func() time.Time
}

type RuntimeData struct {
	DownloadBytes    int64                   `json:"downloadBytes"`
	UploadBytes      int64                   `json:"uploadBytes"`
	DownloadRate     int64                   `json:"downloadRate"`
	UploadRate       int64                   `json:"uploadRate"`
	Peers            int                     `json:"peers"`
	PathStats        PathStats               `json:"pathStats"`
	PeerStats        map[string]PeerStats    `json:"peerStats,omitempty"`
	ActiveDownloads  []ActiveDownload        `json:"activeDownloads,omitempty"`
	RecentDecisions  []SelectionDecision     `json:"recentDecisions,omitempty"`
	UDPBurstProfiles []UDPBurstProfileStatus `json:"udpBurstProfiles,omitempty"`
	DownloadSamples  []transferSample        `json:"downloadSamples,omitempty"`
	UploadSamples    []transferSample        `json:"uploadSamples,omitempty"`
}

type PeerStats struct {
	DownloadedBytes  int64 `json:"downloadedBytes"`
	UploadedBytes    int64 `json:"uploadedBytes"`
	DownloadedPieces int   `json:"downloadedPieces"`
	UploadedPieces   int   `json:"uploadedPieces"`
}

type ActiveDownload struct {
	PieceIndex int    `json:"pieceIndex"`
	PeerID     string `json:"peerId"`
	WorkerID   int    `json:"workerId"`
	StartedAt  string `json:"startedAt"`
}

type SelectionDecision struct {
	PieceIndex           int     `json:"pieceIndex"`
	SelectedPeerID       string  `json:"selectedPeerId"`
	SelectedTransport    string  `json:"selectedTransport"`
	SelectedScore        float64 `json:"selectedScore"`
	SelectedBurstProfile string  `json:"selectedBurstProfile,omitempty"`
	SelectedLastStage    string  `json:"selectedLastStage,omitempty"`
	SelectedUDPBudget    int     `json:"selectedUdpBudget,omitempty"`
	SelectedUDPTimeoutMs int64   `json:"selectedUdpTimeoutMs,omitempty"`
	TopUDPPeerID         string  `json:"topUdpPeerId,omitempty"`
	TopUDPScore          float64 `json:"topUdpScore,omitempty"`
	Reason               string  `json:"reason"`
	RecordedAt           string  `json:"recordedAt"`
}

type UDPBurstProfileStatus struct {
	PeerID        string `json:"peerId"`
	Profile       string `json:"profile"`
	LastStage     string `json:"lastStage,omitempty"`
	LastSuccessAt string `json:"lastSuccessAt,omitempty"`
	LastFailureAt string `json:"lastFailureAt,omitempty"`
	FailureCount  int    `json:"failureCount"`
}

type transferSample struct {
	AtUnixMilli int64 `json:"atUnixMilli"`
	Bytes       int64 `json:"bytes"`
}

const rateWindow = 5 * time.Second

func OpenRuntimeStats(root string) (*RuntimeStats, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}

	stats := &RuntimeStats{
		path: filepath.Join(root, "runtime.json"),
		now:  time.Now,
	}
	if err := stats.load(); err != nil {
		return nil, err
	}
	return stats, nil
}

func (r *RuntimeStats) Snapshot() RuntimeData {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot := cloneRuntimeData(r.data)
	now := r.now()
	snapshot.DownloadSamples = pruneSamples(snapshot.DownloadSamples, now, rateWindow)
	snapshot.UploadSamples = pruneSamples(snapshot.UploadSamples, now, rateWindow)
	snapshot.DownloadRate = calculateRate(snapshot.DownloadSamples, now)
	snapshot.UploadRate = calculateRate(snapshot.UploadSamples, now)
	return snapshot
}

func (r *RuntimeStats) SetPeers(peers int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.Peers = peers
	return r.saveLocked()
}

func (r *RuntimeStats) RecordDownload(bytes int64, path string, peerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	r.data.DownloadBytes += bytes
	applyPathBytes(&r.data.PathStats, path, bytes)
	ensurePeerStatsMap(&r.data)
	stats := r.data.PeerStats[peerID]
	stats.DownloadedBytes += bytes
	stats.DownloadedPieces++
	r.data.PeerStats[peerID] = stats
	r.data.DownloadSamples = append(r.data.DownloadSamples, transferSample{
		AtUnixMilli: now.UnixMilli(),
		Bytes:       bytes,
	})
	r.data.DownloadSamples = pruneSamples(r.data.DownloadSamples, now, rateWindow)
	r.data.DownloadRate = calculateRate(r.data.DownloadSamples, now)
	return r.saveLocked()
}

func (r *RuntimeStats) StartDownload(pieceIndex int, peerID string, workerID int, startedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data.ActiveDownloads = removeActiveDownload(r.data.ActiveDownloads, pieceIndex)
	r.data.ActiveDownloads = append(r.data.ActiveDownloads, ActiveDownload{
		PieceIndex: pieceIndex,
		PeerID:     peerID,
		WorkerID:   workerID,
		StartedAt:  startedAt.Format(time.RFC3339),
	})
	sort.Slice(r.data.ActiveDownloads, func(i, j int) bool {
		if r.data.ActiveDownloads[i].WorkerID != r.data.ActiveDownloads[j].WorkerID {
			return r.data.ActiveDownloads[i].WorkerID < r.data.ActiveDownloads[j].WorkerID
		}
		return r.data.ActiveDownloads[i].PieceIndex < r.data.ActiveDownloads[j].PieceIndex
	})
	return r.saveLocked()
}

func (r *RuntimeStats) RecordSelectionDecision(decision SelectionDecision) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data.RecentDecisions = append(r.data.RecentDecisions, decision)
	if len(r.data.RecentDecisions) > 20 {
		r.data.RecentDecisions = append([]SelectionDecision(nil), r.data.RecentDecisions[len(r.data.RecentDecisions)-20:]...)
	}
	return r.saveLocked()
}

func (r *RuntimeStats) SetUDPBurstProfiles(profiles []UDPBurstProfileStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(profiles) == 0 {
		r.data.UDPBurstProfiles = nil
		return r.saveLocked()
	}
	r.data.UDPBurstProfiles = append([]UDPBurstProfileStatus(nil), profiles...)
	return r.saveLocked()
}

func (r *RuntimeStats) FinishDownload(pieceIndex int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data.ActiveDownloads = removeActiveDownload(r.data.ActiveDownloads, pieceIndex)
	return r.saveLocked()
}

func (r *RuntimeStats) RecordUpload(bytes int64, path string, peerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	r.data.UploadBytes += bytes
	applyPathBytes(&r.data.PathStats, path, bytes)
	ensurePeerStatsMap(&r.data)
	stats := r.data.PeerStats[peerID]
	stats.UploadedBytes += bytes
	stats.UploadedPieces++
	r.data.PeerStats[peerID] = stats
	r.data.UploadSamples = append(r.data.UploadSamples, transferSample{
		AtUnixMilli: now.UnixMilli(),
		Bytes:       bytes,
	})
	r.data.UploadSamples = pruneSamples(r.data.UploadSamples, now, rateWindow)
	r.data.UploadRate = calculateRate(r.data.UploadSamples, now)
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

func ensurePeerStatsMap(data *RuntimeData) {
	if data.PeerStats == nil {
		data.PeerStats = make(map[string]PeerStats)
	}
}

func pruneSamples(samples []transferSample, now time.Time, window time.Duration) []transferSample {
	if len(samples) == 0 {
		return nil
	}

	cutoff := now.Add(-window).UnixMilli()
	filtered := samples[:0]
	for _, sample := range samples {
		if sample.AtUnixMilli < cutoff {
			continue
		}
		filtered = append(filtered, sample)
	}
	return filtered
}

func calculateRate(samples []transferSample, now time.Time) int64 {
	if len(samples) == 0 {
		return 0
	}

	var total int64
	for _, sample := range samples {
		total += sample.Bytes
	}

	oldest := time.UnixMilli(samples[0].AtUnixMilli)
	elapsed := now.Sub(oldest)
	if elapsed < time.Second {
		elapsed = time.Second
	}
	return int64(float64(total) / elapsed.Seconds())
}

func cloneRuntimeData(input RuntimeData) RuntimeData {
	output := input
	if len(input.ActiveDownloads) > 0 {
		output.ActiveDownloads = append([]ActiveDownload(nil), input.ActiveDownloads...)
	}
	if len(input.RecentDecisions) > 0 {
		output.RecentDecisions = append([]SelectionDecision(nil), input.RecentDecisions...)
	}
	if len(input.UDPBurstProfiles) > 0 {
		output.UDPBurstProfiles = append([]UDPBurstProfileStatus(nil), input.UDPBurstProfiles...)
	}
	if len(input.DownloadSamples) > 0 {
		output.DownloadSamples = append([]transferSample(nil), input.DownloadSamples...)
	}
	if len(input.UploadSamples) > 0 {
		output.UploadSamples = append([]transferSample(nil), input.UploadSamples...)
	}
	if input.PeerStats != nil {
		output.PeerStats = make(map[string]PeerStats, len(input.PeerStats))
		for key, value := range input.PeerStats {
			output.PeerStats[key] = value
		}
	}
	return output
}

func removeActiveDownload(active []ActiveDownload, pieceIndex int) []ActiveDownload {
	if len(active) == 0 {
		return active
	}

	filtered := active[:0]
	for _, item := range active {
		if item.PieceIndex == pieceIndex {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}
