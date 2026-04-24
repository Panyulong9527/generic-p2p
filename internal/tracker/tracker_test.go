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

func TestTrackerPersistsAndReloadsState(t *testing.T) {
	statePath := t.TempDir() + "\\tracker-state.json"

	server := NewServer().WithStatePath(statePath)
	server.cleanupInterval = 0
	httpServer := httptest.NewServer(server.Handler())

	client := NewClient(httpServer.URL)
	ctx := context.Background()

	if err := client.RegisterPeer(ctx, "peer-a", []string{"127.0.0.1:9001"}); err != nil {
		t.Fatal(err)
	}
	if err := client.JoinSwarm(ctx, "peer-a", "sha256-demo", []core.HaveRange{{Start: 0, End: 2}}); err != nil {
		t.Fatal(err)
	}
	httpServer.Close()

	reloaded := NewServer().WithStatePath(statePath)
	reloaded.cleanupInterval = 0
	if err := reloaded.loadState(); err != nil {
		t.Fatal(err)
	}

	if got := reloaded.peers["peer-a"].PeerID; got != "peer-a" {
		t.Fatalf("expected persisted peer, got %q", got)
	}
	if !reloaded.swarms["sha256-demo"]["peer-a"] {
		t.Fatal("expected persisted swarm membership")
	}
}

func TestTrackerStatusIncludesSwarmDetails(t *testing.T) {
	server := NewServer()
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	client := NewClient(httpServer.URL)
	ctx := context.Background()

	if err := client.RegisterPeer(ctx, "peer-a", []string{"127.0.0.1:9001"}); err != nil {
		t.Fatal(err)
	}
	if err := client.JoinSwarm(ctx, "peer-a", "sha256-demo", []core.HaveRange{{Start: 0, End: 3}}); err != nil {
		t.Fatal(err)
	}

	status, err := client.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.PeerCount != 1 {
		t.Fatalf("unexpected peer count: %d", status.PeerCount)
	}
	if status.SwarmCount != 1 {
		t.Fatalf("unexpected swarm count: %d", status.SwarmCount)
	}
	if len(status.Swarms) != 1 {
		t.Fatalf("unexpected swarm status count: %d", len(status.Swarms))
	}
	if status.Swarms[0].ContentID != "sha256-demo" {
		t.Fatalf("unexpected swarm content id: %s", status.Swarms[0].ContentID)
	}
	if status.Swarms[0].PeerCount != 1 {
		t.Fatalf("unexpected swarm peer count: %d", status.Swarms[0].PeerCount)
	}
	if len(status.Swarms[0].Peers) != 1 || status.Swarms[0].Peers[0].PeerID != "peer-a" {
		t.Fatalf("unexpected swarm peers: %#v", status.Swarms[0].Peers)
	}
}
