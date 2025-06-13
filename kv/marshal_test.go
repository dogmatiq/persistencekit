package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
)

func TestStore(t *testing.T) {
	store := NewMarshalingStore(
		&memorykv.BinaryStore{},
		marshaler.NewJSON[string](),
		marshaler.NewJSON[int](),
	)

	ks, err := store.Open(t.Context(), "<name>")
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	pairs := map[string]int{
		"one": 1,
		"two": 2,
	}

	for k, v := range pairs {
		if err := ks.Set(t.Context(), k, v); err != nil {
			t.Fatal(err)
		}
	}

	fn := func(_ context.Context, k string, v int) (bool, error) {
		expect := pairs[k]
		if v != expect {
			t.Fatalf("unexpected value for key %q: got %d, want %d", k, v, expect)
		}
		return true, nil
	}

	if err := ks.Range(t.Context(), fn); err != nil {
		t.Fatal(err)
	}

	for k := range pairs {
		ok, err := ks.Has(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected key %q to exist", k)
		}

		v, err := ks.Get(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		fn(t.Context(), k, v)

		if err := ks.Set(t.Context(), k, 0); err != nil {
			t.Fatal(err)
		}

		ok, err = ks.Has(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("expected key %q to be deleted", k)
		}

		ok, err = ks.Has(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("expected key %q to be deleted", k)
		}
	}

	if err := ks.Range(
		t.Context(),
		func(_ context.Context, k string, v int) (bool, error) {
			return false, fmt.Errorf("unexpected range function invocation (%q, %d)", k, v)
		},
	); err != nil {
		t.Fatal(err)
	}
}
