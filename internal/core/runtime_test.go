package core

import "testing"

func TestRuntimeStatsPersist(t *testing.T) {
	dir := t.TempDir()

	stats, err := OpenRuntimeStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := stats.SetPeers(3); err != nil {
		t.Fatal(err)
	}
	if err := stats.RecordDownload(100, "lan"); err != nil {
		t.Fatal(err)
	}
	if err := stats.RecordUpload(50, "direct"); err != nil {
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
}
