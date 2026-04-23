package net

import (
	"errors"
	"fmt"
	stdnet "net"
	"time"

	"generic-p2p/internal/core"
)

type Client struct {
	Addr    string
	Timeout time.Duration
}

func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		Addr:    addr,
		Timeout: timeout,
	}
}

func (c *Client) FetchManifest(contentID string) (*core.ContentManifest, error) {
	request, err := EncodeMessage(MessageTypeManifestRequest, ManifestRequest{ContentID: contentID})
	if err != nil {
		return nil, err
	}
	response, err := c.roundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.Type != MessageTypeManifestResponse {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}
	body, err := DecodeBody[ManifestResponse](response)
	if err != nil {
		return nil, err
	}
	return body.Manifest, nil
}

func (c *Client) FetchHave(contentID string) ([]core.HaveRange, error) {
	request, err := EncodeMessage(MessageTypeHaveRequest, HaveRequest{ContentID: contentID})
	if err != nil {
		return nil, err
	}
	response, err := c.roundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.Type != MessageTypeHaveResponse {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}
	body, err := DecodeBody[HaveResponse](response)
	if err != nil {
		return nil, err
	}
	return body.HaveRanges, nil
}

func (c *Client) FetchPiece(contentID string, pieceIndex int) ([]byte, error) {
	request, err := EncodeMessage(MessageTypePieceRequest, PieceRequest{
		ContentID:  contentID,
		PieceIndex: pieceIndex,
	})
	if err != nil {
		return nil, err
	}
	response, err := c.roundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.Type != MessageTypePieceData {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}
	body, err := DecodeBody[PieceData](response)
	if err != nil {
		return nil, err
	}
	return body.Data, nil
}

func (c *Client) roundTrip(request Message) (Message, error) {
	conn, err := stdnet.DialTimeout("tcp", c.Addr, c.Timeout)
	if err != nil {
		return Message{}, err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(c.Timeout))

	if err := WriteMessage(conn, request); err != nil {
		return Message{}, err
	}
	response, err := ReadMessage(conn)
	if err != nil {
		return Message{}, err
	}
	if response.Type == MessageTypeError {
		body, decodeErr := DecodeBody[ErrorMessage](response)
		if decodeErr != nil {
			return Message{}, decodeErr
		}
		return Message{}, errors.New(body.Message)
	}
	return response, nil
}
