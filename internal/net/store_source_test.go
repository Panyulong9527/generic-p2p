package net

import (
	"os"
	"path/filepath"
	"testing"

	"generic-p2p/internal/core"
)

func TestStoreContentSourceAdvertisesCompletedRanges(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.bin")
	content := []byte("store backed source test data")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := core.BuildManifestFromFile(sourcePath, 8)
	if err != nil {
		t.Fatal(err)
	}

	store, err := core.OpenPieceStore(filepath.Join(dir, "store"), manifest)
	if err != nil {
		t.Fatal(err)
	}

	first := manifest.Pieces[0]
	chunk := content[first.Offset : first.Offset+first.Length]
	if err := store.PutPiece(first.Index, chunk); err != nil {
		t.Fatal(err)
	}

	source := StoreContentSource{Store: store}
	ranges, err := source.HaveRanges(manifest.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	if !core.ContainsPiece(ranges, first.Index) {
		t.Fatalf("expected piece %d to be advertised", first.Index)
	}

	data, err := source.Piece(manifest.ContentID, first.Index)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.VerifyPieceData(first, data); err != nil {
		t.Fatal(err)
	}
}
