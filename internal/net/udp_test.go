package net

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"generic-p2p/internal/core"
)

func TestUDPClientFetchHaveAndPiece(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.txt")
	if err := os.WriteFile(sourcePath, []byte("hello udp transport"), 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := core.BuildManifestFromFile(sourcePath, 5)
	if err != nil {
		t.Fatal(err)
	}

	addr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewUDPServer(addr, StaticContentSource{
		ManifestFile: manifest,
		FilePath:     sourcePath,
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()
	waitForUDPServer(t, addr, manifest.ContentID)

	client := NewUDPClient(addr, time.Second)
	haveRanges, err := client.FetchHave(manifest.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	if !core.ContainsPiece(haveRanges, 2) {
		t.Fatalf("expected have ranges to include piece 2: %#v", haveRanges)
	}

	piece, err := client.FetchPiece(manifest.ContentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(piece) != " udp " {
		t.Fatalf("unexpected piece data: %q", string(piece))
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("udp server did not stop")
	}
}

func freeUDPAddr(t *testing.T) string {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().String()
}

func waitForUDPServer(t *testing.T, addr string, contentID string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client := NewUDPClient(addr, 50*time.Millisecond)
		_, err := client.FetchHave(contentID)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return
	}
}
