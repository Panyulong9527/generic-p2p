package scheduler

import "generic-p2p/internal/core"

type PeerCandidate struct {
	PeerID               string
	Addr                 string
	Transport            string
	IsLAN                bool
	Score                float64
	PeerTopologyRole     string
	PathAssistScore      float64
	BurstProfile         string
	UDPDecisionRisk      string
	UDPPublicMapped      bool
	UDPChunkSamples      int
	UDPChunkReceiveRatio float64
	UDPChunkCompleteRate float64
	UDPChunkDurationMs   int64
	HaveRanges           []core.HaveRange
	PendingCount         int
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
	if preferCandidateByTopologyRole(candidate, current) {
		return true
	}
	if preferCurrentByTopologyRole(candidate, current) {
		return false
	}
	if preferCandidateByPublicMappedUDP(candidate, current) {
		return true
	}
	if preferCurrentByPublicMappedUDP(candidate, current) {
		return false
	}
	if preferCandidateByChunkHealthyUDP(candidate, current) {
		return true
	}
	if preferCurrentByChunkHealthyUDP(candidate, current) {
		return false
	}
	if preferCandidateByChunkWeakness(candidate, current) {
		return true
	}
	if preferCurrentByChunkWeakness(candidate, current) {
		return false
	}
	if preferCandidateByTransportRisk(candidate, current) {
		return true
	}
	if preferCurrentByTransportRisk(candidate, current) {
		return false
	}
	if candidate.IsLAN != current.IsLAN {
		return candidate.IsLAN
	}
	if candidate.PathAssistScore != current.PathAssistScore {
		return candidate.PathAssistScore > current.PathAssistScore
	}
	if candidate.Score != current.Score {
		return candidate.Score > current.Score
	}
	return candidate.PendingCount < current.PendingCount
}

func preferCandidateByTopologyRole(candidate PeerCandidate, current PeerCandidate) bool {
	if topologyRolePriority(candidate.PeerTopologyRole) >= topologyRolePriority(current.PeerTopologyRole) {
		return false
	}
	return candidate.Score >= current.Score-topologyRolePreferenceMargin(candidate.PeerTopologyRole, current.PeerTopologyRole)
}

func preferCurrentByTopologyRole(candidate PeerCandidate, current PeerCandidate) bool {
	if topologyRolePriority(current.PeerTopologyRole) >= topologyRolePriority(candidate.PeerTopologyRole) {
		return false
	}
	return current.Score >= candidate.Score-topologyRolePreferenceMargin(current.PeerTopologyRole, candidate.PeerTopologyRole)
}

func topologyRolePriority(role string) int {
	switch role {
	case "bulk":
		return 0
	case "backup":
		return 1
	case "assist":
		return 2
	case "fallback":
		return 3
	default:
		return 2
	}
}

func topologyRolePreferenceMargin(preferred string, other string) float64 {
	switch preferred {
	case "bulk":
		if other == "fallback" {
			return 0.40
		}
		return 0.24
	case "backup":
		if other == "fallback" {
			return 0.28
		}
		return 0.16
	case "assist":
		if other == "fallback" {
			return 0.12
		}
		return 0.08
	default:
		return 0
	}
}

func preferCandidateByTransportRisk(candidate PeerCandidate, current PeerCandidate) bool {
	return candidate.Transport == "tcp" &&
		current.Transport == "udp" &&
		isSuppressedUDPRisk(current.UDPDecisionRisk) &&
		candidate.Score >= current.Score-udpRiskTCPPreferenceMargin(current.UDPDecisionRisk)
}

func preferCurrentByTransportRisk(candidate PeerCandidate, current PeerCandidate) bool {
	return current.Transport == "tcp" &&
		candidate.Transport == "udp" &&
		isSuppressedUDPRisk(candidate.UDPDecisionRisk) &&
		current.Score >= candidate.Score-udpRiskTCPPreferenceMargin(candidate.UDPDecisionRisk)
}

func isSuppressedUDPRisk(risk string) bool {
	switch risk {
	case "low", "warn":
		return true
	default:
		return false
	}
}

func udpRiskTCPPreferenceMargin(risk string) float64 {
	switch risk {
	case "low":
		return 0.35
	case "warn":
		return 0.18
	default:
		return 0
	}
}

func preferCandidateByPublicMappedUDP(candidate PeerCandidate, current PeerCandidate) bool {
	return candidate.Transport == "udp" &&
		candidate.UDPPublicMapped &&
		!isSuppressedUDPRisk(candidate.UDPDecisionRisk) &&
		current.Transport == "tcp" &&
		candidate.Score >= current.Score-publicMappedUDPPreferenceMargin(candidate)
}

func preferCurrentByPublicMappedUDP(candidate PeerCandidate, current PeerCandidate) bool {
	return current.Transport == "udp" &&
		current.UDPPublicMapped &&
		!isSuppressedUDPRisk(current.UDPDecisionRisk) &&
		candidate.Transport == "tcp" &&
		current.Score >= candidate.Score-publicMappedUDPPreferenceMargin(current)
}

func publicMappedUDPPreferenceMargin(candidate PeerCandidate) float64 {
	switch candidate.UDPDecisionRisk {
	case "stable":
		return 0.28
	case "recovering":
		return 0.18
	default:
		return 0.12
	}
}

func preferCandidateByChunkHealthyUDP(candidate PeerCandidate, current PeerCandidate) bool {
	return candidate.Transport == "udp" &&
		current.Transport == "tcp" &&
		!isSuppressedUDPRisk(candidate.UDPDecisionRisk) &&
		udpChunkLooksHealthy(candidate) &&
		candidate.Score >= current.Score-udpChunkHealthyPreferenceMargin(candidate)
}

func preferCurrentByChunkHealthyUDP(candidate PeerCandidate, current PeerCandidate) bool {
	return current.Transport == "udp" &&
		candidate.Transport == "tcp" &&
		!isSuppressedUDPRisk(current.UDPDecisionRisk) &&
		udpChunkLooksHealthy(current) &&
		current.Score >= candidate.Score-udpChunkHealthyPreferenceMargin(current)
}

func preferCandidateByChunkWeakness(candidate PeerCandidate, current PeerCandidate) bool {
	return candidate.Transport == "tcp" &&
		current.Transport == "udp" &&
		udpChunkLooksWeak(current) &&
		candidate.Score >= current.Score-udpChunkWeakTCPPreferenceMargin(current)
}

func preferCurrentByChunkWeakness(candidate PeerCandidate, current PeerCandidate) bool {
	return current.Transport == "tcp" &&
		candidate.Transport == "udp" &&
		udpChunkLooksWeak(candidate) &&
		current.Score >= candidate.Score-udpChunkWeakTCPPreferenceMargin(candidate)
}

func udpChunkLooksHealthy(candidate PeerCandidate) bool {
	return candidate.UDPChunkSamples >= 2 &&
		candidate.UDPChunkCompleteRate >= 0.65 &&
		candidate.UDPChunkReceiveRatio >= 0.82
}

func udpChunkLooksWeak(candidate PeerCandidate) bool {
	return candidate.UDPChunkSamples >= 2 &&
		candidate.UDPChunkReceiveRatio < 0.4
}

func udpChunkHealthyPreferenceMargin(candidate PeerCandidate) float64 {
	switch candidate.UDPDecisionRisk {
	case "stable":
		return 0.22
	case "recovering":
		return 0.14
	default:
		return 0.10
	}
}

func udpChunkWeakTCPPreferenceMargin(candidate PeerCandidate) float64 {
	switch candidate.UDPDecisionRisk {
	case "low":
		return 0.28
	case "warn":
		return 0.18
	default:
		return 0.14
	}
}
