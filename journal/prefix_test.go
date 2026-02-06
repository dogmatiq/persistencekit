package journal_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestWithNamePrefix(t *testing.T) {
	var underlying memoryjournal.Store[int]

	store := WithNamePrefix(&underlying, "prefix-")

	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	t.Run("it opens the underlying journal with the prefixed name", func(t *testing.T) {
		const want = 42

		if err := j.Append(t.Context(), 0, want); err != nil {
			t.Fatal(err)
		}

		u, err := underlying.Open(t.Context(), "prefix-test")
		if err != nil {
			t.Fatal(err)
		}

		got, err := u.Get(t.Context(), 0)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Errorf("unexpected record: got %d, want %d", got, want)
		}
	})

	t.Run("it reports the unprefixed name", func(t *testing.T) {
		if got, want := j.Name(), "test"; got != want {
			t.Errorf("unexpected name: got %q, want %q", got, want)
		}
	})
}
