package set_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/marshaler"
	. "github.com/dogmatiq/persistencekit/set"
)

func TestStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := NewMarshalingStore(
		&memoryset.BinaryStore{},
		marshaler.NewJSON[int](),
	)

	set, err := store.Open(ctx, "<name>")
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	// add [0, 5)
	for v := range 5 {
		if err := set.Add(ctx, v); err != nil {
			t.Fatal(err)
		}
	}

	// try-add [0, 10)
	for v := range 10 {
		want := v >= 5
		got, err := set.TryAdd(ctx, v)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Fatalf("unexpected membership for %d: got %t, want %t", v, got, want)
		}
	}

	// remove [0, 3)
	for v := range 3 {
		if err := set.Remove(ctx, v); err != nil {
			t.Fatal(err)
		}
	}

	// try-remove [0, 5)
	for v := range 5 {
		want := v >= 3
		got, err := set.TryRemove(ctx, v)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Fatalf("unexpected membership for %d: got %t, want %t", v, got, want)
		}
	}

	// expect set to contain [5, 9]
	for v := range 10 {
		want := v >= 5
		got, err := set.Has(ctx, v)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Fatalf("unexpected membership for %d: got %t, want %t", v, got, want)
		}
	}
}
