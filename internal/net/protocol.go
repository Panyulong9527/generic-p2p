package net

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"generic-p2p/internal/core"
)

const (
	MessageTypeManifestRequest  = "MANIFEST_REQUEST"
	MessageTypeManifestResponse = "MANIFEST_RESPONSE"
	MessageTypeHaveRequest      = "HAVE_REQUEST"
	MessageTypeHaveResponse     = "HAVE_RESPONSE"
	MessageTypePieceRequest     = "PIECE_REQUEST"
	MessageTypePieceData        = "PIECE_DATA"
	MessageTypeError            = "ERROR"
)

type Message struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body,omitempty"`
}

type ManifestRequest struct {
	ContentID string `json:"contentId"`
}

type ManifestResponse struct {
	Manifest *core.ContentManifest `json:"manifest"`
}

type HaveRequest struct {
	ContentID string `json:"contentId"`
}

type HaveResponse struct {
	ContentID  string           `json:"contentId"`
	HaveRanges []core.HaveRange `json:"haveRanges"`
}

type PieceRequest struct {
	ContentID  string `json:"contentId"`
	PieceIndex int    `json:"pieceIndex"`
}

type PieceData struct {
	ContentID  string `json:"contentId"`
	PieceIndex int    `json:"pieceIndex"`
	Data       []byte `json:"data"`
}

type ErrorMessage struct {
	Message string `json:"message"`
}

func EncodeMessage(messageType string, body any) (Message, error) {
	if body == nil {
		return Message{Type: messageType}, nil
	}
	data, err := json.Marshal(body)
	if err != nil {
		return Message{}, err
	}
	return Message{Type: messageType, Body: data}, nil
}

func DecodeBody[T any](message Message) (T, error) {
	var body T
	if len(message.Body) == 0 {
		return body, fmt.Errorf("message %s has empty body", message.Type)
	}
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return body, err
	}
	return body, nil
}

func WriteMessage(w io.Writer, message Message) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	length := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func ReadMessage(r io.Reader) (Message, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return Message{}, err
	}
	if length == 0 {
		return Message{}, fmt.Errorf("message length is zero")
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Message{}, err
	}
	var message Message
	if err := json.Unmarshal(data, &message); err != nil {
		return Message{}, err
	}
	return message, nil
}
