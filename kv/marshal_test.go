package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
)

func TestNewMarshalingStore(t *testing.T) {
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

	pairs := map[string]*struct {
		Value    int
		Revision uint64
	}{
		"one": {Value: 1},
		"two": {Value: 2},
	}

	for k, p := range pairs {
		if err := ks.Set(t.Context(), k, p.Value, p.Revision); err != nil {
			t.Fatal(err)
		}
		p.Revision++
	}

	fn := func(_ context.Context, k string, actualValue int, actualRevision uint64) (bool, error) {
		expect := pairs[k]

		if actualValue != expect.Value {
			t.Fatalf("unexpected value for key %q: got %d, want %d", k, actualValue, expect.Value)
		}

		if actualRevision != expect.Revision {
			t.Fatalf("unexpected revision for key %q: got %d, want %d", k, actualRevision, expect.Revision)
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

		actualValue, actualRevision, err := ks.Get(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		fn(t.Context(), k, actualValue, actualRevision)

		if err := ks.Set(t.Context(), k, 0, actualRevision); err != nil {
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
		func(_ context.Context, k string, v int, _ uint64) (bool, error) {
			return false, fmt.Errorf("unexpected range function invocation (%q, %d)", k, v)
		},
	); err != nil {
		t.Fatal(err)
	}
}
