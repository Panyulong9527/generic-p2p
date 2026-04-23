package main

import "testing"

func TestPeerUsageStateTracksAssignments(t *testing.T) {
	state := newPeerUsageState()

	if count := state.AssignmentCount("peer-a"); count != 0 {
		t.Fatalf("expected zero initial assignments, got %d", count)
	}

	state.RecordAssignment("peer-a")
	state.RecordAssignment("peer-a")

	if count := state.AssignmentCount("peer-a"); count != 2 {
		t.Fatalf("expected 2 assignments, got %d", count)
	}
}
