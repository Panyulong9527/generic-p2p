package main

import "sync"

type peerLoadState struct {
	mu      sync.Mutex
	pending map[string]int
}

func newPeerLoadState() *peerLoadState {
	return &peerLoadState{
		pending: make(map[string]int),
	}
}

func (s *peerLoadState) PendingCount(peerID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending[peerID]
}

func (s *peerLoadState) Acquire(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[peerID]++
}

func (s *peerLoadState) Release(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending[peerID] <= 1 {
		delete(s.pending, peerID)
		return
	}
	s.pending[peerID]--
}
