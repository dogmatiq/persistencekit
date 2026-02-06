package kv_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/persistencekit/kv"
)

func TestWithNameTransform(t *testing.T) {
	var untransformed memorykv.Store[string, int]

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
		const (
			key   = "<key>"
			value = 42
		)

		if err := x.SetUnconditional(t.Context(), key, value); err != nil {
			t.Fatal(err)
		}

		got, _, err := u.Get(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}

		if got != value {
			t.Errorf("unexpected value: got %d, want %d", got, value)
		}
	})
}
