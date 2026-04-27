package core

import (
	"testing"
	"time"
)

func TestRuntimeStatsPersist(t *testing.T) {
	dir := t.TempDir()

	stats, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	baseTime := time.Unix(1700000000, 0)
	stats.now = func() time.Time { return baseTime }
	if err := stats.SetPeers(3); err != nil {
		t.Fatal(err)
	}
	if err := stats.StartDownload(7, "peer-a", 2, baseTime); err != nil {
		t.Fatal(err)
	}
	if err := stats.RecordDownload(100, "lan", "peer-a"); err != nil {
		t.Fatal(err)
	}
	if err := stats.RecordUpload(50, "direct", "peer-b"); err != nil {
		t.Fatal(err)
	}
	if err := stats.FinishDownload(7); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = func() time.Time { return baseTime }
	snapshot := reopened.Snapshot()
	if snapshot.Peers != 3 {
		t.Fatalf("unexpected peers: %d", snapshot.Peers)
	}
	if snapshot.DownloadBytes != 100 {
		t.Fatalf("unexpected download bytes: %d", snapshot.DownloadBytes)
	}
	if snapshot.UploadBytes != 50 {
		t.Fatalf("unexpected upload bytes: %d", snapshot.UploadBytes)
	}
	if snapshot.DownloadRate != 100 {
		t.Fatalf("unexpected download rate: %d", snapshot.DownloadRate)
	}
	if snapshot.UploadRate != 50 {
		t.Fatalf("unexpected upload rate: %d", snapshot.UploadRate)
	}
	if snapshot.PathStats.LANBytes != 100 {
		t.Fatalf("unexpected lan bytes: %d", snapshot.PathStats.LANBytes)
	}
	if snapshot.PathStats.DirectBytes != 50 {
		t.Fatalf("unexpected direct bytes: %d", snapshot.PathStats.DirectBytes)
	}
	if snapshot.PeerStats["peer-a"].DownloadedBytes != 100 {
		t.Fatalf("unexpected peer-a downloaded bytes: %d", snapshot.PeerStats["peer-a"].DownloadedBytes)
	}
	if snapshot.PeerStats["peer-a"].DownloadedPieces != 1 {
		t.Fatalf("unexpected peer-a downloaded pieces: %d", snapshot.PeerStats["peer-a"].DownloadedPieces)
	}
	if snapshot.PeerStats["peer-b"].UploadedBytes != 50 {
		t.Fatalf("unexpected peer-b uploaded bytes: %d", snapshot.PeerStats["peer-b"].UploadedBytes)
	}
	if snapshot.PeerStats["peer-b"].UploadedPieces != 1 {
		t.Fatalf("unexpected peer-b uploaded pieces: %d", snapshot.PeerStats["peer-b"].UploadedPieces)
	}
	if len(snapshot.ActiveDownloads) != 0 {
		t.Fatalf("expected no active downloads after finish, got %+v", snapshot.ActiveDownloads)
	}
}

func TestRuntimeStatsPersistsActiveDownloads(t *testing.T) {
	dir := t.TempDir()

	stats, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := stats.StartDownload(3, "peer-z", 4, time.Unix(1700000001, 0)); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	if len(snapshot.ActiveDownloads) != 1 {
		t.Fatalf("expected one active download, got %+v", snapshot.ActiveDownloads)
	}
	if snapshot.ActiveDownloads[0].PieceIndex != 3 || snapshot.ActiveDownloads[0].PeerID != "peer-z" || snapshot.ActiveDownloads[0].WorkerID != 4 {
		t.Fatalf("unexpected active download: %+v", snapshot.ActiveDownloads[0])
	}
}

func TestRuntimeStatsPersistsRecentSelectionDecisions(t *testing.T) {
	dir := t.TempDir()

	stats, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	decision := SelectionDecision{
		PieceIndex:        5,
		SelectedPeerID:    "peer-tcp",
		SelectedTransport: "tcp",
		SelectedScore:     1.35,
		TopUDPPeerID:      "udp://peer-a",
		TopUDPScore:       1.10,
		Reason:            "selected_tcp_over_lower_udp_score",
		RecordedAt:        time.Unix(1700000200, 0).Format(time.RFC3339),
	}
	if err := stats.RecordSelectionDecision(decision); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	if len(snapshot.RecentDecisions) != 1 {
		t.Fatalf("expected one recent decision, got %+v", snapshot.RecentDecisions)
	}
	if snapshot.RecentDecisions[0].Reason != decision.Reason || snapshot.RecentDecisions[0].TopUDPPeerID != decision.TopUDPPeerID {
		t.Fatalf("unexpected recent decision: %+v", snapshot.RecentDecisions[0])
	}
}

func TestRuntimeStatsPersistsUDPBurstProfiles(t *testing.T) {
	dir := t.TempDir()

	stats, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	profiles := []UDPBurstProfileStatus{
		{
			PeerID:        "peer-a",
			Profile:       "warm",
			LastSuccessAt: time.Unix(1700000300, 0).Format(time.RFC3339),
		},
		{
			PeerID:        "peer-b",
			Profile:       "aggressive",
			LastFailureAt: time.Unix(1700000310, 0).Format(time.RFC3339),
			FailureCount:  2,
		},
	}
	if err := stats.SetUDPBurstProfiles(profiles); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	if len(snapshot.UDPBurstProfiles) != 2 {
		t.Fatalf("expected two udp burst profiles, got %+v", snapshot.UDPBurstProfiles)
	}
	if snapshot.UDPBurstProfiles[0].PeerID != "peer-a" || snapshot.UDPBurstProfiles[1].FailureCount != 2 {
		t.Fatalf("unexpected udp burst profiles: %+v", snapshot.UDPBurstProfiles)
	}
}

func TestRuntimeStatsRateFallsToZeroAfterWindow(t *testing.T) {
	dir := t.TempDir()

	stats, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}

	currentTime := time.Unix(1700000100, 0)
	stats.now = func() time.Time { return currentTime }

	if err := stats.RecordDownload(500, "lan", "peer-a"); err != nil {
		t.Fatal(err)
	}

	if got := stats.Snapshot().DownloadRate; got != 500 {
		t.Fatalf("expected initial download rate 500, got %d", got)
	}

	currentTime = currentTime.Add(6 * time.Second)
	if got := stats.Snapshot().DownloadRate; got != 0 {
		t.Fatalf("expected stale download rate to decay to 0, got %d", got)
	}
}
