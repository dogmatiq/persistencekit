package hrw

import (
	"slices"

	"github.com/cespare/xxhash/v2"
)

// Select returns the candidate with the highest score for the given workload.
func Select(candidates []string, workload string) string {
	if len(candidates) == 0 {
		panic("candidate list must not be empty")
	}

	if len(candidates) == 1 {
		return candidates[0]
	}

	var (
		hash   xxhash.Digest
		winner string
		score  uint64
	)

	for _, c := range candidates {
		hash.Reset()
		hash.WriteString(c)
		hash.WriteString(workload)

		if s := hash.Sum64(); s > score {
			winner = c
			score = s
		}
	}

	return winner
}

// Rank returns a copy of the candidate list, sorted in order of preference for
// the given workload.
func Rank(candidates []string, workload string) []string {
	candidates = slices.Clone(candidates)
	if len(candidates) <= 1 {
		return candidates
	}

	var (
		hash   xxhash.Digest
		scores = make(map[string]uint64, len(candidates))
	)

	for _, c := range candidates {
		hash.Reset()
		hash.WriteString(c)
		hash.WriteString(workload)

		scores[c] = hash.Sum64()
	}

	slices.SortStableFunc(
		candidates,
		func(a, b string) int {
			sa := scores[a]
			sb := scores[b]

			if sa > sb {
				return -1
			} else if sa < sb {
				return +1
			}
			return 0
		},
	)

	return candidates
}
