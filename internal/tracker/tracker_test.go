package tracker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

func TestTrackerStoresUDPAddrs(t *testing.T) {
	server := NewServer()
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	client := NewClient(httpServer.URL)
	ctx := context.Background()

	if err := client.RegisterPeerWithUDP(ctx, "peer-a", []string{"127.0.0.1:9001"}, []string{"127.0.0.1:9003"}); err != nil {
		t.Fatal(err)
	}
	if err := client.JoinSwarm(ctx, "peer-a", "sha256-demo", []core.HaveRange{{Start: 0, End: 1}}); err != nil {
		t.Fatal(err)
	}

	peers, err := client.GetPeers(ctx, "sha256-demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Fatalf("unexpected peer count: %d", len(peers))
	}
	if len(peers[0].UDPAddrs) != 1 || peers[0].UDPAddrs[0] != "127.0.0.1:9003" {
		t.Fatalf("unexpected udp addrs: %#v", peers[0].UDPAddrs)
	}
	if peers[0].ObservedAddr == "" {
		t.Fatal("expected observed tcp addr")
	}
	if peers[0].ObservedUDPAddr == "" {
		t.Fatal("expected observed udp addr")
	}
	if !strings.HasSuffix(peers[0].ObservedAddr, ":9001") {
		t.Fatalf("expected observed tcp addr to keep declared port, got %s", peers[0].ObservedAddr)
	}
	if !strings.HasSuffix(peers[0].ObservedUDPAddr, ":9003") {
		t.Fatalf("expected observed udp addr to keep declared port, got %s", peers[0].ObservedUDPAddr)
	}
}

func TestTrackerCoordinatesUDPProbeRequests(t *testing.T) {
	server := NewServer()
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	client := NewClient(httpServer.URL)
	ctx := context.Background()

	if err := client.RegisterPeerWithUDP(ctx, "peer-a", []string{"127.0.0.1:9001"}, []string{"127.0.0.1:9003"}); err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterPeerWithUDP(ctx, "peer-b", []string{"127.0.0.1:9002"}, []string{"127.0.0.1:9004"}); err != nil {
		t.Fatal(err)
	}
	if err := client.RequestUDPProbe(ctx, "sha256-demo", "peer-b", "127.0.0.1:9004", "peer-a"); err != nil {
		t.Fatal(err)
	}

	requests, err := client.PollUDPProbeRequests(ctx, "peer-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("unexpected request count: %d", len(requests))
	}
	if requests[0].RequesterPeerID != "peer-b" {
		t.Fatalf("unexpected requester peer id: %s", requests[0].RequesterPeerID)
	}
	if requests[0].ObservedUDPAddr == "" {
		t.Fatal("expected observed udp addr")
	}
	if !strings.HasSuffix(requests[0].ObservedUDPAddr, ":9004") {
		t.Fatalf("expected observed udp addr to keep requester port, got %s", requests[0].ObservedUDPAddr)
	}

	requests, err = client.PollUDPProbeRequests(ctx, "peer-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 0 {
		t.Fatalf("expected probe requests to be consumed, got %#v", requests)
	}

	if err := client.ReportUDPProbeResult(ctx, "peer-a", "peer-b", "sha256-demo", false, "udp_timeout"); err != nil {
		t.Fatal(err)
	}
	status, err := client.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.RecentUDPProbeFailures != 1 {
		t.Fatalf("unexpected recent udp probe failures: %d", status.RecentUDPProbeFailures)
	}
	if len(status.UDPProbeResults) != 1 || status.UDPProbeResults[0].LastErrorKind != "udp_timeout" {
		t.Fatalf("unexpected udp probe results: %#v", status.UDPProbeResults)
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
	if err := client.RequestUDPProbe(ctx, "sha256-demo", "peer-b", "127.0.0.1:9004", "peer-a"); err != nil {
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
	if status.PendingUDPProbeCount != 1 {
		t.Fatalf("unexpected pending udp probe count: %d", status.PendingUDPProbeCount)
	}
	if len(status.PendingUDPProbes) != 1 || status.PendingUDPProbes[0].TargetPeerID != "peer-a" {
		t.Fatalf("unexpected pending udp probe status: %#v", status.PendingUDPProbes)
	}
	if err := client.ReportUDPProbeResult(ctx, "peer-a", "peer-b", "sha256-demo", true, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.ReportTransferPath(ctx, "peer-a", "sha256-demo", "udp"); err != nil {
		t.Fatal(err)
	}
	if err := client.ReportTransferPath(ctx, "peer-a", "sha256-demo", "tcp"); err != nil {
		t.Fatal(err)
	}
	status, err = client.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.RecentUDPProbeSuccesses != 1 {
		t.Fatalf("unexpected recent udp probe successes: %d", status.RecentUDPProbeSuccesses)
	}
	if len(status.UDPProbeResults) != 1 || status.UDPProbeResults[0].SuccessCount != 1 {
		t.Fatalf("unexpected udp probe result status: %#v", status.UDPProbeResults)
	}
	if len(status.PeerTransferPaths) != 1 {
		t.Fatalf("unexpected peer transfer path count: %#v", status.PeerTransferPaths)
	}
	if status.PeerTransferPaths[0].TargetPeerID != "peer-a" || status.PeerTransferPaths[0].LastPath != "tcp" {
		t.Fatalf("unexpected peer transfer path status: %#v", status.PeerTransferPaths)
	}
	if status.PeerTransferPaths[0].UDPCount != 1 || status.PeerTransferPaths[0].TCPCount != 1 {
		t.Fatalf("unexpected transfer path counters: %#v", status.PeerTransferPaths[0])
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

func TestTrackerWebAuthProtectsWebRoutes(t *testing.T) {
	dir := t.TempDir()
	usersPath := dir + "\\users.json"
	if err := os.WriteFile(usersPath, []byte(`{"users":[{"username":"admin","password":"secret"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}

	server := NewServer().
		WithWebDataDir(dir + "\\web").
		WithWebUsersPath(usersPath)
	if err := server.loadWebUsers(); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	noRedirectClient := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	pageReq, err := http.NewRequest(http.MethodGet, httpServer.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	pageReq.Header.Set("Accept", "text/html")
	pageResp, err := noRedirectClient.Do(pageReq)
	if err != nil {
		t.Fatal(err)
	}
	defer pageResp.Body.Close()
	if pageResp.StatusCode != http.StatusFound {
		t.Fatalf("expected unauthenticated page redirect, got %s", pageResp.Status)
	}

	apiResp, err := noRedirectClient.Get(httpServer.URL + "/v1/web/shares")
	if err != nil {
		t.Fatal(err)
	}
	defer apiResp.Body.Close()
	if apiResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthenticated API rejection, got %s", apiResp.Status)
	}

	loginResp, err := noRedirectClient.Post(
		httpServer.URL+"/login",
		"application/x-www-form-urlencoded",
		strings.NewReader("username=admin&password=secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusFound {
		t.Fatalf("unexpected login status: %s", loginResp.Status)
	}
	if len(loginResp.Cookies()) == 0 {
		t.Fatal("expected login cookie")
	}

	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/web/shares", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, cookie := range loginResp.Cookies() {
		req.AddCookie(cookie)
	}
	authResp, err := noRedirectClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer authResp.Body.Close()
	if authResp.StatusCode != http.StatusOK {
		t.Fatalf("expected authenticated API success, got %s", authResp.Status)
	}
}
