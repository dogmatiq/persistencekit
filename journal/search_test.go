package journal_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestSearch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := &memoryjournal.Store[int]{}
	j, err := store.Open(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	for i := 0; i < 100; i++ {
		if err := j.Append(ctx, Position(i), i); err != nil {
			t.Fatal(err)
		}
	}

	datum := 55
	fn := func(
		ctx context.Context,
		pos Position,
		rec int,
	) (int, error) {
		return rec - datum, nil
	}

	pos, rec, err := Search(ctx, j, 0, 100, fn)
	if err != nil {
		t.Fatal(err)
	}

	expect := Position(55)
	if pos != expect {
		t.Fatalf("unexpected position: got %d, want %d", pos, expect)
	}

	if rec != datum {
		t.Fatalf("unexpected record: got %d, want %d", rec, datum)
	}

	datum = 101
	if _, _, err = Search(ctx, j, 0, 100, fn); err != ErrNotFound {
		t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
	}
}
