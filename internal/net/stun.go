package net

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	stdnet "net"
	"strings"
	"time"
)

const (
	stunBindingRequest              = 0x0001
	stunBindingSuccess              = 0x0101
	stunHeaderBytes                 = 20
	stunMagicCookie          uint32 = 0x2112A442
	stunAttrMappedAddress           = 0x0001
	stunAttrXORMappedAddress        = 0x0020
	defaultSTUNPort                 = "3478"
)

func DiscoverSTUNMappedAddr(serverAddr string, localAddr string, timeout time.Duration) (string, error) {
	serverAddr = normalizeSTUNServerAddr(serverAddr)
	if strings.TrimSpace(serverAddr) == "" {
		return "", errors.New("stun server address is required")
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	transactionID, err := newSTUNTransactionID()
	if err != nil {
		return "", err
	}
	request := buildSTUNBindingRequest(transactionID)
	conn, remote, err := openSTUNConn(serverAddr, localAddr)
	if err != nil {
		return "", err
	}
	if conn == nil {
		return discoverSTUNWithSharedSocket(remote, strings.TrimSpace(localAddr), request, transactionID, timeout)
	}
	defer conn.Close()

	if _, err := conn.WriteToUDP(request, remote); err != nil {
		return "", err
	}
	return readSTUNResponse(conn, serverAddr, transactionID, timeout)
}

func openSTUNConn(serverAddr string, localAddr string) (*stdnet.UDPConn, *stdnet.UDPAddr, error) {
	remote, err := stdnet.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, nil, err
	}
	localAddr = strings.TrimSpace(localAddr)
	if localAddr != "" {
		if _, ok := lookupUDPSharedSocket(localAddr); ok {
			return nil, remote, nil
		}
	}
	var local *stdnet.UDPAddr
	if localAddr != "" {
		local, err = stdnet.ResolveUDPAddr("udp", localAddr)
		if err != nil {
			return nil, nil, err
		}
	}
	conn, err := stdnet.ListenUDP("udp", local)
	if err != nil {
		return nil, nil, err
	}
	return conn, remote, nil
}

func discoverSTUNWithSharedSocket(remote *stdnet.UDPAddr, localAddr string, request []byte, transactionID []byte, timeout time.Duration) (string, error) {
	socket, ok := lookupUDPSharedSocket(localAddr)
	if !ok {
		return "", fmt.Errorf("shared udp socket unavailable for %s", localAddr)
	}
	key := hex.EncodeToString(transactionID)
	msgCh, release := socket.registerRaw(key)
	defer release()
	if _, err := socket.conn.WriteToUDP(request, remote); err != nil {
		return "", err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return "", &UDPTimeoutError{Op: "stun", Addr: remote.String(), Err: errors.New("stun deadline exceeded")}
		case payload := <-msgCh:
			addr, err := parseSTUNMappedAddress(payload, transactionID)
			if err != nil {
				continue
			}
			return addr, nil
		}
	}
}

func readSTUNResponse(conn *stdnet.UDPConn, serverAddr string, transactionID []byte, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	buffer := make([]byte, 1024)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return "", err
		}
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return "", wrapUDPReadError("stun", serverAddr, err)
		}
		addr, err := parseSTUNMappedAddress(buffer[:n], transactionID)
		if err != nil {
			continue
		}
		return addr, nil
	}
}

func buildSTUNBindingRequest(transactionID []byte) []byte {
	payload := make([]byte, stunHeaderBytes)
	binary.BigEndian.PutUint16(payload[0:2], stunBindingRequest)
	binary.BigEndian.PutUint16(payload[2:4], 0)
	binary.BigEndian.PutUint32(payload[4:8], stunMagicCookie)
	copy(payload[8:20], transactionID)
	return payload
}

func parseSTUNMappedAddress(payload []byte, transactionID []byte) (string, error) {
	if len(payload) < stunHeaderBytes {
		return "", errors.New("stun response too short")
	}
	if binary.BigEndian.Uint16(payload[0:2]) != stunBindingSuccess {
		return "", errors.New("not a stun binding success response")
	}
	if binary.BigEndian.Uint32(payload[4:8]) != stunMagicCookie {
		return "", errors.New("invalid stun magic cookie")
	}
	if len(transactionID) != 12 || string(payload[8:20]) != string(transactionID) {
		return "", errors.New("stun transaction id mismatch")
	}
	messageLength := int(binary.BigEndian.Uint16(payload[2:4]))
	if len(payload) < stunHeaderBytes+messageLength {
		return "", errors.New("truncated stun response")
	}

	attrs := payload[20 : 20+messageLength]
	for len(attrs) >= 4 {
		attrType := binary.BigEndian.Uint16(attrs[0:2])
		attrLen := int(binary.BigEndian.Uint16(attrs[2:4]))
		if len(attrs) < 4+attrLen {
			return "", errors.New("truncated stun attribute")
		}
		value := attrs[4 : 4+attrLen]
		switch attrType {
		case stunAttrXORMappedAddress:
			if addr, err := decodeSTUNXORMappedAddress(value, transactionID); err == nil {
				return addr, nil
			}
		case stunAttrMappedAddress:
			if addr, err := decodeSTUNMappedAddress(value); err == nil {
				return addr, nil
			}
		}
		padded := (attrLen + 3) &^ 3
		if len(attrs) < 4+padded {
			break
		}
		attrs = attrs[4+padded:]
	}
	return "", errors.New("mapped address not found")
}

func decodeSTUNMappedAddress(value []byte) (string, error) {
	if len(value) < 4 {
		return "", errors.New("mapped address too short")
	}
	family := value[1]
	port := binary.BigEndian.Uint16(value[2:4])
	switch family {
	case 0x01:
		if len(value) < 8 {
			return "", errors.New("mapped ipv4 address too short")
		}
		ip := stdnet.IPv4(value[4], value[5], value[6], value[7])
		return stdnet.JoinHostPort(ip.String(), fmt.Sprintf("%d", port)), nil
	case 0x02:
		if len(value) < 20 {
			return "", errors.New("mapped ipv6 address too short")
		}
		ip := stdnet.IP(append([]byte(nil), value[4:20]...))
		return stdnet.JoinHostPort(ip.String(), fmt.Sprintf("%d", port)), nil
	default:
		return "", fmt.Errorf("unsupported stun family %d", family)
	}
}

func decodeSTUNXORMappedAddress(value []byte, transactionID []byte) (string, error) {
	if len(value) < 4 {
		return "", errors.New("xor mapped address too short")
	}
	family := value[1]
	port := binary.BigEndian.Uint16(value[2:4]) ^ uint16(stunMagicCookie>>16)
	switch family {
	case 0x01:
		if len(value) < 8 {
			return "", errors.New("xor mapped ipv4 address too short")
		}
		cookie := make([]byte, 4)
		binary.BigEndian.PutUint32(cookie, stunMagicCookie)
		ip := stdnet.IPv4(
			value[4]^cookie[0],
			value[5]^cookie[1],
			value[6]^cookie[2],
			value[7]^cookie[3],
		)
		return stdnet.JoinHostPort(ip.String(), fmt.Sprintf("%d", port)), nil
	case 0x02:
		if len(value) < 20 {
			return "", errors.New("xor mapped ipv6 address too short")
		}
		mask := make([]byte, 16)
		binary.BigEndian.PutUint32(mask[:4], stunMagicCookie)
		copy(mask[4:], transactionID)
		ip := make([]byte, 16)
		for i := 0; i < 16; i++ {
			ip[i] = value[4+i] ^ mask[i]
		}
		return stdnet.JoinHostPort(stdnet.IP(ip).String(), fmt.Sprintf("%d", port)), nil
	default:
		return "", fmt.Errorf("unsupported stun family %d", family)
	}
}

func newSTUNTransactionID() ([]byte, error) {
	transactionID := make([]byte, 12)
	if _, err := rand.Read(transactionID); err != nil {
		return nil, err
	}
	return transactionID, nil
}

func normalizeSTUNServerAddr(serverAddr string) string {
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		return ""
	}
	if _, _, err := stdnet.SplitHostPort(serverAddr); err == nil {
		return serverAddr
	}
	if strings.Count(serverAddr, ":") > 1 && !strings.HasPrefix(serverAddr, "[") {
		return stdnet.JoinHostPort(serverAddr, defaultSTUNPort)
	}
	return stdnet.JoinHostPort(serverAddr, defaultSTUNPort)
}

func stunTransactionKey(payload []byte) (string, bool) {
	if len(payload) < stunHeaderBytes {
		return "", false
	}
	if payload[0]&0xC0 != 0 {
		return "", false
	}
	if binary.BigEndian.Uint32(payload[4:8]) != stunMagicCookie {
		return "", false
	}
	return hex.EncodeToString(payload[8:20]), true
}
