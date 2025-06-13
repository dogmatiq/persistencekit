package journal_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/marshaler"
)

func TestMarshal(t *testing.T) {
	store := NewMarshalingStore(
		&memoryjournal.BinaryStore{},
		marshaler.NewJSON[int](),
	)

	j, err := store.Open(t.Context(), "<name>")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	if err := j.Append(t.Context(), 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := j.Append(t.Context(), 1, 101); err != nil {
		t.Fatal(err)
	}

	fn := func(_ context.Context, pos Position, rec int) (bool, error) {
		expect := int(pos) + 100
		if rec != expect {
			t.Fatalf("unexpected value at position %d: got %d, want %d", pos, rec, expect)
		}
		return true, nil
	}

	if err := j.Range(t.Context(), 0, fn); err != nil {
		t.Fatal(err)
	}

	for pos := Position(0); pos < 2; pos++ {
		rec, err := j.Get(t.Context(), pos)
		if err != nil {
			t.Fatal(err)
		}
		fn(t.Context(), pos, rec)
	}
}
