package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyPieceData(t *testing.T) {
	data := []byte("piece-data")
	piece := PieceInfo{
		Index:  0,
		Length: int64(len(data)),
		Hash:   HashBytes(data),
	}

	if err := VerifyPieceData(piece, data); err != nil {
		t.Fatalf("expected piece verification to succeed: %v", err)
	}
	if err := VerifyPieceData(piece, []byte("bad")); err == nil {
		t.Fatal("expected piece verification to fail")
	}
}

func TestPieceStoreResumeAndAssemble(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.bin")
	content := []byte("abcdefghijklmnopqrstuvwxyz")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := BuildManifestFromFile(sourcePath, 5)
	if err != nil {
		t.Fatal(err)
	}

	storeRoot := filepath.Join(dir, "store")
	store, err := OpenPieceStore(storeRoot, manifest)
	if err != nil {
		t.Fatal(err)
	}

	for _, piece := range manifest.Pieces {
		chunk := content[piece.Offset : piece.Offset+piece.Length]
		if err := store.PutPiece(piece.Index, chunk); err != nil {
			t.Fatalf("put piece %d: %v", piece.Index, err)
		}
	}

	reopened, err := OpenPieceStore(storeRoot, manifest)
	if err != nil {
		t.Fatal(err)
	}
	status := reopened.Status()
	if status.CompletedPieces != len(manifest.Pieces) {
		t.Fatalf("unexpected completed pieces: %d", status.CompletedPieces)
	}

	outPath := filepath.Join(dir, "out.bin")
	if err := reopened.AssembleTo(outPath); err != nil {
		t.Fatal(err)
	}

	outData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(outData) != string(content) {
		t.Fatalf("assembled content mismatch: %q != %q", string(outData), string(content))
	}
}
