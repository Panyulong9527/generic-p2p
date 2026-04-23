package net

import (
	"bytes"
	"testing"

	"generic-p2p/internal/core"
)

func TestMessageRoundTrip(t *testing.T) {
	message, err := EncodeMessage(MessageTypePieceRequest, PieceRequest{
		ContentID:  "sha256-demo",
		PieceIndex: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	var buffer bytes.Buffer
	if err := WriteMessage(&buffer, message); err != nil {
		t.Fatal(err)
	}

	got, err := ReadMessage(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != MessageTypePieceRequest {
		t.Fatalf("unexpected message type: %s", got.Type)
	}
	body, err := DecodeBody[PieceRequest](got)
	if err != nil {
		t.Fatal(err)
	}
	if body.PieceIndex != 3 {
		t.Fatalf("unexpected piece index: %d", body.PieceIndex)
	}
}

func TestHaveMessageRoundTrip(t *testing.T) {
	message, err := EncodeMessage(MessageTypeHaveResponse, HaveResponse{
		ContentID: "sha256-demo",
		HaveRanges: []core.HaveRange{
			{Start: 0, End: 2},
			{Start: 4, End: 6},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var buffer bytes.Buffer
	if err := WriteMessage(&buffer, message); err != nil {
		t.Fatal(err)
	}

	got, err := ReadMessage(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	body, err := DecodeBody[HaveResponse](got)
	if err != nil {
		t.Fatal(err)
	}
	if len(body.HaveRanges) != 2 {
		t.Fatalf("unexpected range count: %d", len(body.HaveRanges))
	}
}
