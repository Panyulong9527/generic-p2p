package main

import "sync"

type peerUsageState struct {
	mu          sync.Mutex
	assignments map[string]int
}

func newPeerUsageState() *peerUsageState {
	return &peerUsageState{
		assignments: make(map[string]int),
	}
}

func (s *peerUsageState) AssignmentCount(peerID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.assignments[peerID]
}

func (s *peerUsageState) RecordAssignment(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assignments[peerID]++
}
