package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
)

const DefaultPieceSize int64 = 1 << 20

func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func VerifyPieceData(piece PieceInfo, data []byte) error {
	if int64(len(data)) != piece.Length {
		return fmt.Errorf("piece %d length mismatch: expected %d, got %d", piece.Index, piece.Length, len(data))
	}
	if HashBytes(data) != piece.Hash {
		return fmt.Errorf("piece %d hash mismatch", piece.Index)
	}
	return nil
}

func ReadPieceFromFile(path string, manifest *ContentManifest, index int) ([]byte, error) {
	if err := ValidateManifest(manifest); err != nil {
		return nil, err
	}
	if index < 0 || index >= len(manifest.Pieces) {
		return nil, fmt.Errorf("piece index out of range: %d", index)
	}

	piece := manifest.Pieces[index]
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, piece.Length)
	if _, err := file.ReadAt(data, piece.Offset); err != nil {
		return nil, err
	}
	if err := VerifyPieceData(piece, data); err != nil {
		return nil, err
	}
	return data, nil
}
