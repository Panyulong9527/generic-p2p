package net

import (
	"context"
	"encoding/binary"
	"errors"
	stdnet "net"
	"strings"
	"testing"
	"time"
)

func TestDiscoverSTUNMappedAddr(t *testing.T) {
	serverAddr := startTestSTUNServer(t)
	localAddr := reserveUDPAddr(t)

	mappedAddr, err := DiscoverSTUNMappedAddr(serverAddr, localAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if mappedAddr != localAddr {
		t.Fatalf("expected mapped addr %s, got %s", localAddr, mappedAddr)
	}
}

func TestDiscoverSTUNMappedAddrWithSharedSocket(t *testing.T) {
	serverAddr := startTestSTUNServer(t)
	localAddr := reserveUDPAddr(t)

	server := NewUDPServer(localAddr, StaticContentSource{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()
	waitForUDPSharedSocket(t, localAddr)

	mappedAddr, err := DiscoverSTUNMappedAddr(serverAddr, localAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if mappedAddr != localAddr {
		t.Fatalf("expected mapped addr %s, got %s", localAddr, mappedAddr)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("udp server exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("udp server did not stop")
	}
}

func startTestSTUNServer(t *testing.T) string {
	t.Helper()
	conn, err := stdnet.ListenUDP("udp", &stdnet.UDPAddr{IP: stdnet.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	go func() {
		buffer := make([]byte, 1024)
		for {
			n, remote, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if n < stunHeaderBytes {
				continue
			}
			transactionID := append([]byte(nil), buffer[8:20]...)
			response := buildTestSTUNResponse(transactionID, remote)
			_, _ = conn.WriteToUDP(response, remote)
		}
	}()
	return conn.LocalAddr().String()
}

func buildTestSTUNResponse(transactionID []byte, remote *stdnet.UDPAddr) []byte {
	ip4 := remote.IP.To4()
	attrValue := make([]byte, 8)
	attrValue[1] = 0x01
	binary.BigEndian.PutUint16(attrValue[2:4], uint16(remote.Port)^uint16(stunMagicCookie>>16))
	cookie := make([]byte, 4)
	binary.BigEndian.PutUint32(cookie, stunMagicCookie)
	for i := 0; i < 4; i++ {
		attrValue[4+i] = ip4[i] ^ cookie[i]
	}

	response := make([]byte, 20+4+len(attrValue))
	binary.BigEndian.PutUint16(response[0:2], stunBindingSuccess)
	binary.BigEndian.PutUint16(response[2:4], uint16(4+len(attrValue)))
	binary.BigEndian.PutUint32(response[4:8], stunMagicCookie)
	copy(response[8:20], transactionID)
	binary.BigEndian.PutUint16(response[20:22], stunAttrXORMappedAddress)
	binary.BigEndian.PutUint16(response[22:24], uint16(len(attrValue)))
	copy(response[24:], attrValue)
	return response
}

func reserveUDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := stdnet.ListenUDP("udp", &stdnet.UDPAddr{IP: stdnet.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	addr := conn.LocalAddr().String()
	_ = conn.Close()
	return addr
}

func waitForUDPSharedSocket(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := lookupUDPSharedSocket(addr); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("shared udp socket was not ready for %s", addr)
}

func TestNormalizeSTUNServerAddrAddsDefaultPort(t *testing.T) {
	if got := normalizeSTUNServerAddr("stun.example.com"); got != "stun.example.com:3478" {
		t.Fatalf("unexpected normalized stun addr: %s", got)
	}
	if got := normalizeSTUNServerAddr("127.0.0.1:19302"); got != "127.0.0.1:19302" {
		t.Fatalf("unexpected normalized stun addr: %s", got)
	}
	if got := normalizeSTUNServerAddr(" [::1] "); !strings.Contains(got, "3478") {
		t.Fatalf("expected ipv6 stun addr with default port, got %s", got)
	}
}
