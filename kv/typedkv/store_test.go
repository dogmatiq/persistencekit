package typedkv_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/persistencekit/kv/typedkv"
)

func TestStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := Store[string, int, jsonMarshaler[string], jsonMarshaler[int]]{
		Store: &memorykv.Store{},
	}

	ks, err := store.Open(ctx, "<name>")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer ks.Close()

	pairs := map[string]int{
		"one": 1,
		"two": 2,
	}

	for k, v := range pairs {
		if err := ks.Set(ctx, k, v); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	fn := func(ctx context.Context, k string, v int) (bool, error) {
		expect := pairs[k]
		if v != expect {
			t.Fatalf("unexpected value for key %q: got %d, want %d", k, v, expect)
		}
		return true, nil
	}

	if err := ks.Range(ctx, fn); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for k := range pairs {
		ok, err := ks.Has(ctx, k)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !ok {
			t.Fatalf("expected key %q to exist", k)
		}

		v, ok, err := ks.Get(ctx, k)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !ok {
			t.Fatalf("expected key %q to exist", k)
		}
		fn(ctx, k, v)

		if err := ks.Delete(ctx, k); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		ok, err = ks.Has(ctx, k)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if ok {
			t.Fatalf("expected key %q to be deleted", k)
		}

		_, ok, err = ks.Get(ctx, k)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if ok {
			t.Fatalf("expected key %q to be deleted", k)
		}
	}

	if err := ks.Range(
		ctx,
		func(ctx context.Context, k string, v int) (bool, error) {
			return false, fmt.Errorf("unexpected range function invocation (%q, %d)", k, v)
		},
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

type jsonMarshaler[R any] struct{}

func (m jsonMarshaler[R]) Marshal(rec R) ([]byte, error) {
	return json.Marshal(rec)
}

func (m jsonMarshaler[R]) Unmarshal(data []byte) (R, error) {
	var rec R
	return rec, json.Unmarshal(data, &rec)
}
