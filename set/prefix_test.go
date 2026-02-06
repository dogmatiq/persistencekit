package set_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	. "github.com/dogmatiq/persistencekit/set"
)

func TestWithNamePrefix(t *testing.T) {
	var underlying memoryset.Store[int]

	store := WithNamePrefix(&underlying, "prefix-")

	ks, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("it adds the prefix to the name", func(t *testing.T) {
		const value = 42

		if err := ks.Add(t.Context(), value); err != nil {
			t.Fatal(err)
		}

		u, err := underlying.Open(t.Context(), "prefix-test")
		if err != nil {
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

	t.Run("it reports the unprefixed name", func(t *testing.T) {
		if got, want := ks.Name(), "test"; got != want {
			t.Errorf("unexpected name: got %q, want %q", got, want)
		}
	})
}
