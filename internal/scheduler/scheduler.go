package scheduler

import "generic-p2p/internal/core"

type PeerCandidate struct {
	PeerID       string
	Addr         string
	Transport    string
	IsLAN        bool
	Score        float64
	HaveRanges   []core.HaveRange
	PendingCount int
}

type Scheduler struct{}

func (s Scheduler) ChoosePeer(pieceIndex int, peers []PeerCandidate) (PeerCandidate, bool) {
	var selected PeerCandidate
	found := false

	for _, peer := range peers {
		if !core.ContainsPiece(peer.HaveRanges, pieceIndex) {
			continue
		}
		if !found || betterPeer(peer, selected) {
			selected = peer
			found = true
		}
	}
	return selected, found
}

func (s Scheduler) ChoosePiece(totalPieces int, peers []PeerCandidate, completed map[int]bool, inProgress map[int]bool) (int, bool) {
	bestIndex := -1
	bestAvailability := 0

	for pieceIndex := 0; pieceIndex < totalPieces; pieceIndex++ {
		if completed[pieceIndex] || inProgress[pieceIndex] {
			continue
		}

		availability := pieceAvailability(pieceIndex, peers)
		if availability == 0 {
			continue
		}

		if bestIndex == -1 || availability < bestAvailability || (availability == bestAvailability && pieceIndex < bestIndex) {
			bestIndex = pieceIndex
			bestAvailability = availability
		}
	}

	if bestIndex == -1 {
		return 0, false
	}
	return bestIndex, true
}

func pieceAvailability(pieceIndex int, peers []PeerCandidate) int {
	count := 0
	for _, peer := range peers {
		if core.ContainsPiece(peer.HaveRanges, pieceIndex) {
			count++
		}
	}
	return count
}

func betterPeer(candidate PeerCandidate, current PeerCandidate) bool {
	if candidate.IsLAN != current.IsLAN {
		return candidate.IsLAN
	}
	if candidate.Score != current.Score {
		return candidate.Score > current.Score
	}
	return candidate.PendingCount < current.PendingCount
}
