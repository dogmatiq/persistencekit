package hrw_test

import (
	"slices"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/adaptor/shard/internal/hrw"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func TestSelect(t *testing.T) {
	t.Parallel()

	t.Run("it distributes workloads across all candidates", func(t *testing.T) {
		t.Parallel()

		var candidates []string
		remaining := map[string]struct{}{}
		for range 10 {
			c := uuid.NewString()
			candidates = append(candidates, c)
			remaining[c] = struct{}{}
		}

		start := time.Now()
		timeout := 5 * time.Second

		for len(remaining) != 0 {
			if time.Since(start) > timeout {
				t.Fatal("timed-out waiting for workloads to be distributed")
			}

			c := Select(candidates, uuid.NewString())
			delete(remaining, c)
		}
	})

	t.Run("it consistently chooses the same candidate", func(t *testing.T) {
		t.Parallel()

		var candidates []string
		for range 10 {
			c := uuid.NewString()
			candidates = append(candidates, c)
		}

		for range 10 {
			workload := uuid.NewString()
			expect := Select(candidates, workload)

			for i := range 10 {
				actual := Select(candidates, workload)
				if actual != expect {
					t.Fatalf("attempt #%d: got %q, want %q", i+1, actual, expect)
				}
			}
		}
	})
}

func TestRank(t *testing.T) {
	t.Parallel()

	t.Run("sorts the candidate list by their relative scores", func(t *testing.T) {
		t.Parallel()

		var candidates []string

		for range 10 {
			c := uuid.NewString()
			candidates = append(candidates, c)
		}

		// ensure we handle duplicates correctly
		candidates = append(candidates, candidates...)
		remaining := slices.Clone(candidates)

		workload := uuid.NewString()
		var expect []string

		for len(remaining) != 0 {
			c := Select(remaining, workload)
			expect = append(expect, c)

			i := slices.Index(remaining, c)
			remaining = slices.Delete(remaining, i, i+1)
		}

		actual := Rank(candidates, workload)

		if diff := cmp.Diff(expect, actual); diff != "" {
			t.Fatal(diff)
		}
	})
}
