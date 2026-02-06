package set_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	. "github.com/dogmatiq/persistencekit/set"
)

func TestWithNameTransform(t *testing.T) {
	var untransformed memoryset.Store[int]

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
		const value = 42

		if err := x.Add(t.Context(), value); err != nil {
			t.Fatal(err)
		}

		ok, err := u.Has(t.Context(), value)
		if err != nil {
			t.Fatal(err)
		}

		if !ok {
			t.Errorf("expected set to contain %d", value)
		}
	})
}
