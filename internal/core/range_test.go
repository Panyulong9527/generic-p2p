package core

import (
	"reflect"
	"testing"
)

func TestNormalizeRanges(t *testing.T) {
	input := []HaveRange{
		{Start: 5, End: 8},
		{Start: 0, End: 2},
		{Start: 3, End: 4},
		{Start: 10, End: 10},
	}

	got := NormalizeRanges(input)
	want := []HaveRange{
		{Start: 0, End: 8},
		{Start: 10, End: 10},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected normalized ranges: %#v", got)
	}
}

func TestDiffRanges(t *testing.T) {
	base := []HaveRange{{Start: 0, End: 2}, {Start: 5, End: 5}}
	next := []HaveRange{{Start: 0, End: 3}, {Start: 5, End: 6}}

	got := DiffRanges(base, next)
	want := []HaveRange{{Start: 3, End: 3}, {Start: 6, End: 6}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected diff ranges: %#v", got)
	}
}

func TestContainsPiece(t *testing.T) {
	ranges := []HaveRange{{Start: 1, End: 3}, {Start: 8, End: 10}}
	if !ContainsPiece(ranges, 2) {
		t.Fatal("expected piece 2 to exist")
	}
	if ContainsPiece(ranges, 7) {
		t.Fatal("expected piece 7 to be missing")
	}
}
