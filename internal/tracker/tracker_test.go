package tracker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
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

func TestTrackerStatusReflectsConfiguredTimings(t *testing.T) {
	server := NewServer().
		WithPeerTTL(45 * time.Second).
		WithCleanupInterval(15 * time.Second)

	status := server.Status()
	if status.PeerTTLSeconds != 45 {
		t.Fatalf("unexpected peer TTL seconds: %d", status.PeerTTLSeconds)
	}
	if status.CleanupIntervalSeconds != 15 {
		t.Fatalf("unexpected cleanup interval seconds: %d", status.CleanupIntervalSeconds)
	}
}

func TestTrackerWebShareUploadListAndDownload(t *testing.T) {
	server := NewServer().WithWebDataDir(t.TempDir())
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	pageResp, err := http.Get(httpServer.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if pageResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected page status: %s", pageResp.Status)
	}
	_ = pageResp.Body.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(part, "hello lan share"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	uploadReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/web/shares", &body)
	if err != nil {
		t.Fatal(err)
	}
	uploadReq.Header.Set("Content-Type", writer.FormDataContentType())

	uploadResp, err := http.DefaultClient.Do(uploadReq)
	if err != nil {
		t.Fatal(err)
	}
	defer uploadResp.Body.Close()
	if uploadResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected upload status: %s", uploadResp.Status)
	}

	var share WebShare
	if err := json.NewDecoder(uploadResp.Body).Decode(&share); err != nil {
		t.Fatal(err)
	}
	if share.Name != "hello.txt" {
		t.Fatalf("unexpected share name: %s", share.Name)
	}

	listResp, err := http.Get(httpServer.URL + "/v1/web/shares")
	if err != nil {
		t.Fatal(err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected list status: %s", listResp.Status)
	}

	var listBody struct {
		Shares []WebShare `json:"shares"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listBody); err != nil {
		t.Fatal(err)
	}
	if len(listBody.Shares) != 1 || listBody.Shares[0].ContentID != share.ContentID {
		t.Fatalf("unexpected shares: %#v", listBody.Shares)
	}

	downloadResp, err := http.Get(httpServer.URL + share.DownloadPath)
	if err != nil {
		t.Fatal(err)
	}
	defer downloadResp.Body.Close()
	downloaded, err := io.ReadAll(downloadResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(downloaded) != "hello lan share" {
		t.Fatalf("unexpected downloaded content: %q", string(downloaded))
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/v1/web/shares/"+share.ContentID, nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected delete status: %s", deleteResp.Status)
	}

	listAfterDeleteResp, err := http.Get(httpServer.URL + "/v1/web/shares")
	if err != nil {
		t.Fatal(err)
	}
	defer listAfterDeleteResp.Body.Close()
	var listAfterDeleteBody struct {
		Shares []WebShare `json:"shares"`
	}
	if err := json.NewDecoder(listAfterDeleteResp.Body).Decode(&listAfterDeleteBody); err != nil {
		t.Fatal(err)
	}
	if len(listAfterDeleteBody.Shares) != 0 {
		t.Fatalf("expected no shares after delete, got %#v", listAfterDeleteBody.Shares)
	}

	deletedDownloadResp, err := http.Get(httpServer.URL + share.DownloadPath)
	if err != nil {
		t.Fatal(err)
	}
	defer deletedDownloadResp.Body.Close()
	if deletedDownloadResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected deleted download to be missing, got %s", deletedDownloadResp.Status)
	}
}
