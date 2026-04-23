package net

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"generic-p2p/internal/core"
)

func TestLocalTransferFetchPiece(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.bin")
	content := []byte("tcp transfer smoke data for generic p2p")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := core.BuildManifestFromFile(sourcePath, 8)
	if err != nil {
		t.Fatal(err)
	}

	server := NewServer("127.0.0.1:19091", StaticContentSource{
		ManifestFile: manifest,
		FilePath:     sourcePath,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.Listen(ctx)
	}()
	time.Sleep(150 * time.Millisecond)

	client := NewClient("127.0.0.1:19091", 3*time.Second)
	haveRanges, err := client.FetchHave(manifest.ContentID)
	if err != nil {
		t.Fatalf("fetch have: %v", err)
	}
	if !core.ContainsPiece(haveRanges, 0) {
		t.Fatal("expected piece 0 to be advertised in have ranges")
	}

	for _, piece := range manifest.Pieces {
		data, err := client.FetchPiece(manifest.ContentID, piece.Index)
		if err != nil {
			t.Fatalf("fetch piece %d: %v", piece.Index, err)
		}
		if err := core.VerifyPieceData(piece, data); err != nil {
			t.Fatalf("verify piece %d: %v", piece.Index, err)
		}
	}
}
