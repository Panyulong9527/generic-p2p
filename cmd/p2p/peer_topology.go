package main

import (
	"strings"
	"time"

	"generic-p2p/internal/scheduler"
)

const (
	peerTopologyRoleBulk     = "bulk"
	peerTopologyRoleBackup   = "backup"
	peerTopologyRoleAssist   = "assist"
	peerTopologyRoleFallback = "fallback"
)

func annotatePeerTopology(candidates []scheduler.PeerCandidate, contentID string, now time.Time) []scheduler.PeerCandidate {
	if len(candidates) == 0 {
		return candidates
	}
	annotated := make([]scheduler.PeerCandidate, len(candidates))
	copy(annotated, candidates)
	for i := range annotated {
		annotated[i].PeerTopologyRole = peerTopologyRoleForCandidate(contentID, annotated[i], now)
		annotated[i].PathAssistScore = peerAssistScoreForCandidate(contentID, annotated[i], now)
		if run := udpSessionContentRun(annotated[i].PeerID, contentID, now); run > 0 {
			annotated[i].PathAssistScore += float64(minInt(run, 3)) * 0.12
		}
	}
	return annotated
}

func annotatePeerTopologyForWorker(candidates []scheduler.PeerCandidate, contentID string, workerID int, now time.Time) []scheduler.PeerCandidate {
	annotated := annotatePeerTopology(candidates, contentID, now)
	if workerID < 0 {
		return annotated
	}
	for i := range annotated {
		run := udpSessionOwnerRun(annotated[i].PeerID, contentID, workerID, now)
		if run <= 0 {
			continue
		}
		annotated[i].PathAssistScore += float64(minInt(run, 3)) * 0.18
	}
	return annotated
}

func peerTopologyRoleForCandidate(contentID string, candidate scheduler.PeerCandidate, now time.Time) string {
	if candidate.Transport != "udp" {
		return peerTopologyRoleBackup
	}
	if udpSessionIsQuarantined(candidate.PeerID, now) {
		return peerTopologyRoleFallback
	}
	if stickyRole := udpSessionStickyRole(candidate.PeerID, contentID, now); stickyRole != "" && !isSuppressedDecisionRisk(candidate.UDPDecisionRisk) {
		return stickyRole
	}
	if udpSessionContentRun(candidate.PeerID, contentID, now) >= 2 && !isSuppressedDecisionRisk(candidate.UDPDecisionRisk) {
		return peerTopologyRoleBulk
	}
	if isSuppressedDecisionRisk(candidate.UDPDecisionRisk) {
		return peerTopologyRoleFallback
	}
	if udpChunkLooksWeakCandidate(candidate) || udpSessionStateForPeer(candidate.PeerID, now) == "cooling" {
		return peerTopologyRoleFallback
	}

	sessionState := udpSessionStateForPeer(candidate.PeerID, now)
	switch {
	case sessionState == "active" && (candidate.UDPPublicMapped || udpChunkLooksHealthyCandidate(candidate) || strings.TrimSpace(candidate.UDPDecisionRisk) == "stable"):
		return peerTopologyRoleBulk
	case sessionState == "warm" && (candidate.UDPPublicMapped || udpChunkLooksHealthyCandidate(candidate) || strings.TrimSpace(candidate.UDPDecisionRisk) == "recovering"):
		return peerTopologyRoleBulk
	case sessionState == "active" || sessionState == "warm":
		return peerTopologyRoleAssist
	case candidate.UDPPublicMapped || udpChunkLooksHealthyCandidate(candidate) || strings.TrimSpace(candidate.UDPDecisionRisk) == "stable":
		return peerTopologyRoleBackup
	default:
		return peerTopologyRoleAssist
	}
}

func peerAssistScoreForCandidate(contentID string, candidate scheduler.PeerCandidate, now time.Time) float64 {
	score := candidate.Score
	switch candidate.PeerTopologyRole {
	case peerTopologyRoleBulk:
		score += 0.35
	case peerTopologyRoleBackup:
		score += 0.12
	case peerTopologyRoleAssist:
		score += 0.22
	case peerTopologyRoleFallback:
		score -= 0.18
	}
	if candidate.Transport == "udp" {
		if stickyRole := udpSessionStickyRole(candidate.PeerID, contentID, now); stickyRole != "" {
			switch stickyRole {
			case peerTopologyRoleBulk:
				score += 0.24
			case peerTopologyRoleAssist:
				score += 0.12
			case peerTopologyRoleFallback:
				score -= 0.20
			}
		}
		if udpSessionIsQuarantined(candidate.PeerID, now) {
			score -= 0.24
		}
		if candidate.UDPPublicMapped {
			score += 0.10
		}
		switch udpSessionStateForPeer(candidate.PeerID, now) {
		case "active":
			score += 0.12
		case "warm":
			score += 0.06
		case "cooling":
			score -= 0.10
		}
		if smoothed, ok := smoothedUDPChunkProgress(contentID, candidate.PeerID, now); ok {
			score += (smoothed.ReceiveRatio - 0.5) * 0.30
			score += (smoothed.CompleteRate - 0.5) * 0.20
		}
	}
	return score
}

func peerTopologyRolePriority(role string) int {
	switch strings.TrimSpace(role) {
	case peerTopologyRoleBulk:
		return 0
	case peerTopologyRoleBackup:
		return 1
	case peerTopologyRoleAssist:
		return 2
	case peerTopologyRoleFallback:
		return 3
	default:
		return 2
	}
}

func udpChunkLooksHealthyCandidate(candidate scheduler.PeerCandidate) bool {
	return candidate.UDPChunkSamples >= 2 &&
		candidate.UDPChunkCompleteRate >= 0.65 &&
		candidate.UDPChunkReceiveRatio >= 0.82
}

func udpChunkLooksWeakCandidate(candidate scheduler.PeerCandidate) bool {
	return candidate.UDPChunkSamples >= 2 &&
		candidate.UDPChunkReceiveRatio < 0.4
}
