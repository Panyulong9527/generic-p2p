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
	if err := client.Probe(); err != nil {
		t.Fatal(err)
	}
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

func TestUDPClientCanReuseServerListenSocket(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "reuse.txt")
	if err := os.WriteFile(sourcePath, []byte("reuse udp socket"), 0o644); err != nil {
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

	client := NewUDPClient(addr, time.Second).WithLocalAddr(addr)
	if err := client.Probe(); err != nil {
		t.Fatal(err)
	}
	haveRanges, err := client.FetchHave(manifest.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	if !core.ContainsPiece(haveRanges, 1) {
		t.Fatalf("expected have ranges to include piece 1: %#v", haveRanges)
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

func TestUDPClientProbeBurstForPeer(t *testing.T) {
	addr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewUDPServer(addr, StaticContentSource{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := NewUDPClient(addr, 50*time.Millisecond).ProbeBurstForPeer("content-burst", "peer-burst", 3, 10*time.Millisecond); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	observedAddr, ok := ObservedUDPPeerAddr("content-burst", "peer-burst", time.Second, time.Now())
	if !ok || observedAddr == "" {
		t.Fatal("expected burst probe to store observed udp peer address")
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

func TestNormalizeUDPBurstPhasesDefaultsAndSanitizes(t *testing.T) {
	got := normalizeUDPBurstPhases([]UDPBurstPhase{
		{Attempts: 0, Gap: -time.Second},
		{Attempts: 2, Gap: 250 * time.Millisecond},
	})
	if len(got) != 2 {
		t.Fatalf("expected 2 normalized phases, got %d", len(got))
	}
	if got[0].Attempts != 1 || got[0].Gap != 0 {
		t.Fatalf("expected first phase sanitized to attempts=1 gap=0, got %+v", got[0])
	}
	if got[1].Attempts != 2 || got[1].Gap != 250*time.Millisecond {
		t.Fatalf("expected second phase unchanged, got %+v", got[1])
	}

	defaulted := normalizeUDPBurstPhases(nil)
	if len(defaulted) != 1 || defaulted[0].Attempts != 1 || defaulted[0].Gap != 0 {
		t.Fatalf("expected default single phase, got %+v", defaulted)
	}
}

func TestUDPClientProbeMultiBurstForPeer(t *testing.T) {
	addr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewUDPServer(addr, StaticContentSource{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := NewUDPClient(addr, 100*time.Millisecond).ProbeMultiBurstForPeer("content-multi-burst", "peer-multi-burst", []UDPBurstPhase{
			{Attempts: 2, Gap: 5 * time.Millisecond},
			{Attempts: 1, Gap: 15 * time.Millisecond},
		}); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	observedAddr, ok := ObservedUDPPeerAddr("content-multi-burst", "peer-multi-burst", time.Second, time.Now())
	if !ok || observedAddr == "" {
		t.Fatal("expected multi-burst probe to store observed udp peer address")
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

func TestShouldKeepAliveObservedUDPPeerThrottlesRequests(t *testing.T) {
	addr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewUDPServer(addr, StaticContentSource{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := lookupUDPSharedSocket(addr); ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	now := time.Unix(1000, 0)
	if !ShouldKeepAliveObservedUDPPeer(addr, "content-a", "peer-a", "198.51.100.1:9003", 8*time.Second, now) {
		t.Fatal("expected first keepalive decision to pass")
	}
	if ShouldKeepAliveObservedUDPPeer(addr, "content-a", "peer-a", "198.51.100.1:9003", 8*time.Second, now.Add(3*time.Second)) {
		t.Fatal("expected keepalive decision to be throttled inside interval")
	}
	if !ShouldKeepAliveObservedUDPPeer(addr, "content-a", "peer-a", "198.51.100.1:9003", 8*time.Second, now.Add(9*time.Second)) {
		t.Fatal("expected keepalive decision to pass after interval")
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

func TestRecentUDPSuccessAddrsExpiresAndSorts(t *testing.T) {
	now := time.Unix(2000, 0)
	RememberRecentUDPSuccess("content-recent", "198.51.100.3:9003", now.Add(-5*time.Second))
	RememberRecentUDPSuccess("content-recent", "198.51.100.1:9003", now.Add(-4*time.Second))
	RememberRecentUDPSuccess("content-recent", "198.51.100.2:9003", now.Add(-40*time.Second))

	addrs := RecentUDPSuccessAddrs("content-recent", 30*time.Second, now)
	if len(addrs) != 2 {
		t.Fatalf("expected 2 recent udp success addrs, got %#v", addrs)
	}
	if addrs[0] != "198.51.100.1:9003" || addrs[1] != "198.51.100.3:9003" {
		t.Fatalf("unexpected sorted recent udp success addrs: %#v", addrs)
	}
}

func TestUDPClientProbeFailsWhenPeerUnavailable(t *testing.T) {
	addr := freeUDPAddr(t)
	client := NewUDPClient(addr, 50*time.Millisecond)
	if err := client.Probe(); err == nil {
		t.Fatal("expected probe to fail without a UDP server")
	} else if !IsUDPTimeout(err) {
		t.Fatalf("expected timeout-classified error, got %v", err)
	}
}

func TestUDPProbeStoresObservedPeerAddress(t *testing.T) {
	addr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewUDPServer(addr, StaticContentSource{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := NewUDPClient(addr, 50*time.Millisecond).ProbeForPeer("content-observed", "peer-udp-a"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	observedAddr, ok := ObservedUDPPeerAddr("content-observed", "peer-udp-a", time.Second, time.Now())
	if !ok {
		t.Fatal("expected observed udp peer address to be stored")
	}
	if observedAddr == "" {
		t.Fatal("expected non-empty observed udp peer address")
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
