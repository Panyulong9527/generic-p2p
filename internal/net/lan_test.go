package net

import (
	"context"
	"testing"
	"time"

	"generic-p2p/internal/core"
)

func TestLANAnnouncementRoundTrip(t *testing.T) {
	announcement := BuildLANAnnouncement("peer-a", "127.0.0.1:9001", []LANContent{
		{
			ContentID:  "sha256-a",
			HaveRanges: []core.HaveRange{{Start: 0, End: 3}},
		},
	})

	data, err := MarshalLANAnnouncement(announcement)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseLANAnnouncement(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Listen != "127.0.0.1:9001" {
		t.Fatalf("unexpected listen addr: %s", got.Listen)
	}
	if got.Contents[0].ContentID != "sha256-a" {
		t.Fatalf("unexpected content id: %s", got.Contents[0].ContentID)
	}
}

func TestDiscoverLANFiltersByContentID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		announceCtx, announceCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer announceCancel()
		_ = AnnounceLAN(announceCtx, "127.0.0.1:19021", 50*time.Millisecond, func() LANAnnouncement {
			return BuildLANAnnouncement("peer-a", "127.0.0.1:9001", []LANContent{
				{ContentID: "sha256-match", HaveRanges: []core.HaveRange{{Start: 0, End: 2}}},
				{ContentID: "sha256-other", HaveRanges: []core.HaveRange{{Start: 9, End: 9}}},
			})
		})
	}()

	peers, err := DiscoverLAN(ctx, ":19021", "sha256-match", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) == 0 {
		t.Fatal("expected at least one discovered peer")
	}
	if peers[0].ListenAddr != "127.0.0.1:9001" {
		t.Fatalf("unexpected listen addr: %s", peers[0].ListenAddr)
	}
	if !core.ContainsPiece(peers[0].HaveRanges, 1) {
		t.Fatal("expected discovered ranges to include piece 1")
	}
}
