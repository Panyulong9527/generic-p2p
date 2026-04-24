package tracker

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"generic-p2p/internal/core"
)

func TestTrackerRegisterJoinAndGetPeers(t *testing.T) {
	server := NewServer()
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	client := NewClient(httpServer.URL)
	ctx := context.Background()

	if err := client.RegisterPeer(ctx, "peer-a", []string{"127.0.0.1:9001"}); err != nil {
		t.Fatal(err)
	}
	if err := client.JoinSwarm(ctx, "peer-a", "sha256-demo", []core.HaveRange{{Start: 0, End: 4}}); err != nil {
		t.Fatal(err)
	}

	peers, err := client.GetPeers(ctx, "sha256-demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Fatalf("unexpected peer count: %d", len(peers))
	}
	if peers[0].PeerID != "peer-a" {
		t.Fatalf("unexpected peer id: %s", peers[0].PeerID)
	}
	if len(peers[0].Addrs) != 1 || peers[0].Addrs[0] != "127.0.0.1:9001" {
		t.Fatalf("unexpected peer addrs: %#v", peers[0].Addrs)
	}
	if !core.ContainsPiece(peers[0].HaveRanges, 3) {
		t.Fatal("expected have range to include piece 3")
	}
}

func TestTrackerPrunesExpiredPeers(t *testing.T) {
	server := NewServer()
	server.peerTTL = 20 * time.Millisecond
	server.cleanupInterval = 0

	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	client := NewClient(httpServer.URL)
	ctx := context.Background()

	if err := client.RegisterPeer(ctx, "peer-a", []string{"127.0.0.1:9001"}); err != nil {
		t.Fatal(err)
	}
	if err := client.JoinSwarm(ctx, "peer-a", "sha256-demo", []core.HaveRange{{Start: 0, End: 1}}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)

	peers, err := client.GetPeers(ctx, "sha256-demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected expired peers to be pruned, got %d", len(peers))
	}
}
