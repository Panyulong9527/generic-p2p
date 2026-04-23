package core

import "sort"

type HaveRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

func NormalizeRanges(ranges []HaveRange) []HaveRange {
	if len(ranges) == 0 {
		return nil
	}

	filtered := make([]HaveRange, 0, len(ranges))
	for _, current := range ranges {
		if current.Start > current.End {
			continue
		}
		filtered = append(filtered, current)
	}
	if len(filtered) == 0 {
		return nil
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Start == filtered[j].Start {
			return filtered[i].End < filtered[j].End
		}
		return filtered[i].Start < filtered[j].Start
	})

	merged := []HaveRange{filtered[0]}
	for _, current := range filtered[1:] {
		last := &merged[len(merged)-1]
		if current.Start <= last.End+1 {
			if current.End > last.End {
				last.End = current.End
			}
			continue
		}
		merged = append(merged, current)
	}
	return merged
}

func DiffRanges(base []HaveRange, next []HaveRange) []HaveRange {
	baseSet := ExpandRanges(base)
	nextSet := ExpandRanges(next)

	added := make([]int, 0)
	for piece := range nextSet {
		if !baseSet[piece] {
			added = append(added, piece)
		}
	}
	sort.Ints(added)
	return CompressPieceIndexes(added)
}

func ExpandRanges(ranges []HaveRange) map[int]bool {
	result := make(map[int]bool)
	for _, current := range NormalizeRanges(ranges) {
		for piece := current.Start; piece <= current.End; piece++ {
			result[piece] = true
		}
	}
	return result
}

func CompressPieceIndexes(indexes []int) []HaveRange {
	if len(indexes) == 0 {
		return nil
	}
	sort.Ints(indexes)

	ranges := make([]HaveRange, 0)
	start := indexes[0]
	end := indexes[0]
	for _, index := range indexes[1:] {
		if index <= end+1 {
			if index > end {
				end = index
			}
			continue
		}
		ranges = append(ranges, HaveRange{Start: start, End: end})
		start = index
		end = index
	}
	ranges = append(ranges, HaveRange{Start: start, End: end})
	return ranges
}

func ContainsPiece(ranges []HaveRange, pieceIndex int) bool {
	for _, current := range NormalizeRanges(ranges) {
		if pieceIndex < current.Start {
			return false
		}
		if pieceIndex >= current.Start && pieceIndex <= current.End {
			return true
		}
	}
	return false
}
