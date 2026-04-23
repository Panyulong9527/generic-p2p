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
	if err := stats.SetPeers(3); err != nil {
		t.Fatal(err)
	}
	if err := stats.StartDownload(7, "peer-a", time.Unix(1700000000, 0)); err != nil {
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
	if err := stats.StartDownload(3, "peer-z", time.Unix(1700000001, 0)); err != nil {
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
	if snapshot.ActiveDownloads[0].PieceIndex != 3 || snapshot.ActiveDownloads[0].PeerID != "peer-z" {
		t.Fatalf("unexpected active download: %+v", snapshot.ActiveDownloads[0])
	}
}
