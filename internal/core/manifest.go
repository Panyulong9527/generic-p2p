package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type ContentManifest struct {
	Version    int         `json:"version"`
	ContentID  string      `json:"contentId"`
	Name       string      `json:"name"`
	TotalSize  int64       `json:"totalSize"`
	PieceSize  int64       `json:"pieceSize"`
	Files      []FileEntry `json:"files"`
	Pieces     []PieceInfo `json:"pieces"`
	MerkleRoot string      `json:"merkleRoot,omitempty"`
	CreatedAt  int64       `json:"createdAt"`
}

type FileEntry struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

type PieceInfo struct {
	Index  int    `json:"index"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Hash   string `json:"hash"`
}

func BuildManifestFromFile(path string, pieceSize int64) (*ContentManifest, error) {
	if pieceSize <= 0 {
		return nil, fmt.Errorf("invalid piece size %d", pieceSize)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path is not a regular file: %s", path)
	}

	buffer := make([]byte, pieceSize)
	fullHash := sha256.New()
	pieces := make([]PieceInfo, 0)

	var offset int64
	for index := 0; ; index++ {
		n, readErr := io.ReadFull(file, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return nil, readErr
		}
		if n == 0 {
			break
		}

		chunk := buffer[:n]
		if _, err := fullHash.Write(chunk); err != nil {
			return nil, err
		}
		pieces = append(pieces, PieceInfo{
			Index:  index,
			Offset: offset,
			Length: int64(n),
			Hash:   HashBytes(chunk),
		})
		offset += int64(n)

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	contentID := "sha256-" + hex.EncodeToString(fullHash.Sum(nil))

	return &ContentManifest{
		Version:   1,
		ContentID: contentID,
		Name:      filepath.Base(path),
		TotalSize: info.Size(),
		PieceSize: pieceSize,
		Files: []FileEntry{
			{
				Path:   filepath.Base(path),
				Offset: 0,
				Length: info.Size(),
			},
		},
		Pieces:    pieces,
		CreatedAt: time.Now().Unix(),
	}, nil
}

func WriteManifestFile(path string, manifest *ContentManifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func ReadManifestFile(path string) (*ContentManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest ContentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if err := ValidateManifest(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func ValidateManifest(manifest *ContentManifest) error {
	if manifest == nil {
		return fmt.Errorf("manifest is nil")
	}
	if manifest.ContentID == "" {
		return fmt.Errorf("manifest contentId is empty")
	}
	if manifest.PieceSize <= 0 {
		return fmt.Errorf("manifest pieceSize must be greater than zero")
	}
	if manifest.TotalSize < 0 {
		return fmt.Errorf("manifest totalSize must be non-negative")
	}

	var totalPieceBytes int64
	for i, piece := range manifest.Pieces {
		if piece.Index != i {
			return fmt.Errorf("manifest piece index mismatch at %d", i)
		}
		if piece.Length <= 0 {
			return fmt.Errorf("manifest piece %d has invalid length", i)
		}
		if piece.Hash == "" {
			return fmt.Errorf("manifest piece %d hash is empty", i)
		}
		totalPieceBytes += piece.Length
	}
	if totalPieceBytes != manifest.TotalSize && !(manifest.TotalSize == 0 && len(manifest.Pieces) == 0) {
		return fmt.Errorf("manifest total piece bytes %d does not match total size %d", totalPieceBytes, manifest.TotalSize)
	}
	return nil
}
