package journal_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestWithNameTransform(t *testing.T) {
	var untransformed memoryjournal.Store[int]

	transformed := WithNameTransform(
		&untransformed,
		func(name string) string {
			return "prefix-" + name
		},
	)

	u, err := untransformed.Open(t.Context(), "prefix-test")
	if err != nil {
		t.Fatal(err)
	}

	x, err := transformed.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("it reports the untransformed name", func(t *testing.T) {
		if got, want := x.Name(), "test"; got != want {
			t.Errorf("unexpected name: got %q, want %q", got, want)
		}
	})

	t.Run("operates on the underlying store with the transformed name", func(t *testing.T) {
		const record = 42

		if err := x.Append(t.Context(), 0, record); err != nil {
			t.Fatal(err)
		}

		got, err := u.Get(t.Context(), 0)
		if err != nil {
			t.Fatal(err)
		}

		if got != record {
			t.Errorf("unexpected record: got %d, want %d", got, record)
		}
	})
}
