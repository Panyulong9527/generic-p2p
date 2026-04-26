package main

import "testing"

func TestUDPCandidateScorePrefersObservedAddresses(t *testing.T) {
	preferred := map[string]bool{"198.51.100.10:9003": true}

	if got := udpCandidateScore("198.51.100.10:9003", preferred); got != 1.6 {
		t.Fatalf("expected preferred udp score 1.6, got %.1f", got)
	}
	if got := udpCandidateScore("203.0.113.20:9003", preferred); got != 1.2 {
		t.Fatalf("expected default udp score 1.2, got %.1f", got)
	}
}
