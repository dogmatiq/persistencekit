package set_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/marshaler"
	. "github.com/dogmatiq/persistencekit/set"
)

func TestStore(t *testing.T) {
	store := NewMarshalingStore(
		&memoryset.BinaryStore{},
		marshaler.NewJSON[int](),
	)

	set, err := store.Open(t.Context(), "<name>")
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	// add [0, 5)
	for v := range 5 {
		if err := set.Add(t.Context(), v); err != nil {
			t.Fatal(err)
		}
	}

	// try-add [0, 10)
	for v := range 10 {
		want := v >= 5
		got, err := set.TryAdd(t.Context(), v)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Fatalf("unexpected membership for %d: got %t, want %t", v, got, want)
		}
	}

	// remove [0, 3)
	for v := range 3 {
		if err := set.Remove(t.Context(), v); err != nil {
			t.Fatal(err)
		}
	}

	// try-remove [0, 5)
	for v := range 5 {
		want := v >= 3
		got, err := set.TryRemove(t.Context(), v)
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
		got, err := set.Has(t.Context(), v)
		if err != nil {
			t.Fatal(err)
		}

		if got != want {
			t.Fatalf("unexpected membership for %d: got %t, want %t", v, got, want)
		}
	}
}
