package kv_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/persistencekit/kv"
)

func TestWithNamePrefix(t *testing.T) {
	var underlying memorykv.Store[int, string]

	store := WithNamePrefix(&underlying, "prefix-")

	ks, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("it adds the prefix to the name", func(t *testing.T) {
		const (
			key  = 42
			want = "<value>"
		)

		if err := ks.SetUnconditional(t.Context(), key, want); err != nil {
			t.Fatal(err)
		}

		u, err := underlying.Open(t.Context(), "prefix-test")
		if err != nil {
			t.Fatal(err)
		}

		got, _, err := u.Get(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Errorf("unexpected value: got %q, want %q", got, want)
		}
	})

	t.Run("it reports the unprefixed name", func(t *testing.T) {
		if got, want := ks.Name(), "test"; got != want {
			t.Errorf("unexpected name: got %q, want %q", got, want)
		}
	})
}
