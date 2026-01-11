package kv_test

import (
	"bytes"
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
		Value int
		Token []byte
	}{
		"one": {Value: 1},
		"two": {Value: 2},
	}

	for k, p := range pairs {
		var err error
		p.Token, err = ks.Set(t.Context(), k, p.Value, p.Token)
		if err != nil {
			t.Fatal(err)
		}
	}

	fn := func(_ context.Context, k string, actualValue int, actualToken []byte) (bool, error) {
		expect := pairs[k]

		if actualValue != expect.Value {
			t.Fatalf("unexpected value for key %q: got %d, want %d", k, actualValue, expect.Value)
		}

		if !bytes.Equal(actualToken, expect.Token) {
			t.Fatalf("unexpected token for key %q: got %q, want %q", k, actualToken, expect.Token)
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

		actualValue, actualToken, err := ks.Get(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		fn(t.Context(), k, actualValue, actualToken)

		newToken, err := ks.Set(t.Context(), k, 0, actualToken)
		if err != nil {
			t.Fatal(err)
		}

		if len(newToken) != 0 {
			t.Fatalf("expected empty token after deleting key %q, got %q", k, newToken)
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
		func(_ context.Context, k string, v int, _ []byte) (bool, error) {
			return false, fmt.Errorf("unexpected range function invocation (%q, %d)", k, v)
		},
	); err != nil {
		t.Fatal(err)
	}
}
