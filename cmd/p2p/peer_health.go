package main

import (
	"sync"
	"time"
)

type peerHealthState struct {
	mu           sync.Mutex
	failureCount map[string]int
	coolingUntil map[string]time.Time
}

func newPeerHealthState() *peerHealthState {
	return &peerHealthState{
		failureCount: make(map[string]int),
		coolingUntil: make(map[string]time.Time),
	}
}

func (s *peerHealthState) IsAvailable(peerID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	until, ok := s.coolingUntil[peerID]
	if !ok {
		return true
	}
	if !now.Before(until) {
		delete(s.coolingUntil, peerID)
		return true
	}
	return false
}

func (s *peerHealthState) RemainingCooldown(peerID string, now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	until, ok := s.coolingUntil[peerID]
	if !ok || !now.Before(until) {
		return 0
	}
	return until.Sub(now)
}

func (s *peerHealthState) MarkFailure(peerID string, now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failureCount[peerID]++
	backoff := peerCooldownForFailures(s.failureCount[peerID])
	s.coolingUntil[peerID] = now.Add(backoff)
	return backoff
}

func (s *peerHealthState) MarkSuccess(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.coolingUntil, peerID)
	delete(s.failureCount, peerID)
}

func peerCooldownForFailures(failures int) time.Duration {
	if failures <= 0 {
		return 0
	}

	backoff := 300 * time.Millisecond
	for step := 1; step < failures; step++ {
		backoff *= 2
		if backoff >= 5*time.Second {
			return 5 * time.Second
		}
	}
	return backoff
}
