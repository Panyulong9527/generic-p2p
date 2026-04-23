package net

import (
	"context"
	"encoding/json"
	"fmt"
	stdnet "net"
	"strings"
	"time"

	"generic-p2p/internal/core"
)

const DefaultLANDiscoveryAddr = "255.255.255.255:19001"

type LANContent struct {
	ContentID  string           `json:"contentId"`
	HaveRanges []core.HaveRange `json:"haveRanges"`
}

type LANAnnouncement struct {
	Type      string       `json:"type"`
	PeerID    string       `json:"peerId"`
	Listen    string       `json:"listen"`
	Contents  []LANContent `json:"contents"`
	Timestamp int64        `json:"timestamp"`
}

type DiscoveredPeer struct {
	PeerID     string
	ListenAddr string
	HaveRanges []core.HaveRange
}

func BuildLANAnnouncement(peerID string, listenAddr string, contents []LANContent) LANAnnouncement {
	return LANAnnouncement{
		Type:      "LAN_ANNOUNCE",
		PeerID:    peerID,
		Listen:    listenAddr,
		Contents:  contents,
		Timestamp: time.Now().Unix(),
	}
}

func MarshalLANAnnouncement(announcement LANAnnouncement) ([]byte, error) {
	return json.Marshal(announcement)
}

func ParseLANAnnouncement(data []byte) (LANAnnouncement, error) {
	var announcement LANAnnouncement
	if err := json.Unmarshal(data, &announcement); err != nil {
		return LANAnnouncement{}, err
	}
	if announcement.Type != "LAN_ANNOUNCE" {
		return LANAnnouncement{}, fmt.Errorf("unexpected LAN message type: %s", announcement.Type)
	}
	if strings.TrimSpace(announcement.Listen) == "" {
		return LANAnnouncement{}, fmt.Errorf("lan listen address is empty")
	}
	return announcement, nil
}

func AnnounceLAN(ctx context.Context, targetAddr string, interval time.Duration, build func() LANAnnouncement) error {
	udpAddr, err := stdnet.ResolveUDPAddr("udp4", targetAddr)
	if err != nil {
		return err
	}

	conn, err := stdnet.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	send := func() error {
		payload, err := MarshalLANAnnouncement(build())
		if err != nil {
			return err
		}
		_, err = conn.Write(payload)
		return err
	}

	if err := send(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := send(); err != nil {
				return err
			}
		}
	}
}

func DiscoverLAN(ctx context.Context, listenAddr string, contentID string, selfListenAddr string) ([]DiscoveredPeer, error) {
	packetConn, err := stdnet.ListenPacket("udp4", listenAddr)
	if err != nil {
		return nil, err
	}
	defer packetConn.Close()

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		_ = packetConn.SetReadDeadline(deadline)
	}

	peersByAddr := make(map[string]DiscoveredPeer)
	buffer := make([]byte, 64*1024)

	for {
		n, _, err := packetConn.ReadFrom(buffer)
		if err != nil {
			if netErr, ok := err.(stdnet.Error); ok && netErr.Timeout() {
				break
			}
			return nil, err
		}

		announcement, err := ParseLANAnnouncement(buffer[:n])
		if err != nil {
			continue
		}
		if announcement.Listen == selfListenAddr {
			continue
		}
		for _, content := range announcement.Contents {
			if content.ContentID != contentID {
				continue
			}
			peersByAddr[announcement.Listen] = DiscoveredPeer{
				PeerID:     announcement.PeerID,
				ListenAddr: announcement.Listen,
				HaveRanges: content.HaveRanges,
			}
		}
	}

	peers := make([]DiscoveredPeer, 0, len(peersByAddr))
	for _, peer := range peersByAddr {
		peers = append(peers, peer)
	}
	return peers, nil
}
