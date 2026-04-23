package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildManifestFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.bin")
	data := []byte("hello generic p2p manifest")

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := BuildManifestFromFile(path, 8)
	if err != nil {
		t.Fatal(err)
	}

	if manifest.Name != "sample.bin" {
		t.Fatalf("unexpected manifest name: %s", manifest.Name)
	}
	if manifest.TotalSize != int64(len(data)) {
		t.Fatalf("unexpected total size: %d", manifest.TotalSize)
	}
	if len(manifest.Pieces) != 4 {
		t.Fatalf("unexpected piece count: %d", len(manifest.Pieces))
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("manifest validation failed: %v", err)
	}
}

func TestManifestReadWriteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.bin")
	manifestPath := filepath.Join(dir, "manifest.json")

	if err := os.WriteFile(path, []byte("round trip"), 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := BuildManifestFromFile(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteManifestFile(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}

	loaded, err := ReadManifestFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.ContentID != manifest.ContentID {
		t.Fatalf("unexpected content id: %s != %s", loaded.ContentID, manifest.ContentID)
	}
}
