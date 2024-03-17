package typedkv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/adaptor/typed/typedkv"
	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
)

func TestStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := Store[string, int, typedmarshaler.String, typedmarshaler.JSON[int]]{
		BinaryStore: &memorykv.BinaryStore{},
	}

	ks, err := store.Open(ctx, "<name>")
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	pairs := map[string]int{
		"one": 1,
		"two": 2,
	}

	for k, v := range pairs {
		if err := ks.Set(ctx, k, v); err != nil {
			t.Fatal(err)
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
		t.Fatal(err)
	}

	for k := range pairs {
		ok, err := ks.Has(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected key %q to exist", k)
		}

		v, err := ks.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		fn(ctx, k, v)

		if err := ks.Set(ctx, k, 0); err != nil {
			t.Fatal(err)
		}

		ok, err = ks.Has(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("expected key %q to be deleted", k)
		}

		ok, err = ks.Has(ctx, k)
		if err != nil {
			t.Fatal(err)
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
		t.Fatal(err)
	}
}
