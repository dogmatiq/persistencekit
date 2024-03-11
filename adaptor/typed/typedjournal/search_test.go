package typedjournal_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/adaptor/typed/typedjournal"
	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestBinarySearch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := Store[int, typedmarshaler.JSON[int]]{
		Store: &memoryjournal.Store{},
	}

	j, err := store.Open(ctx, "<name>")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	for pos := journal.Position(0); pos < 100; pos++ {
		if err := j.Append(ctx, pos, int(pos)); err != nil {
			t.Fatal(err)
		}
	}

	datum := 55
	fn := func(
		ctx context.Context,
		pos journal.Position,
		rec int,
	) (int, error) {
		return rec - datum, nil
	}

	pos, rec, err := BinarySearch(ctx, j, 0, 100, fn)
	if err != nil {
		t.Fatal(err)
	}

	expect := journal.Position(55)
	if pos != expect {
		t.Fatalf("unexpected position: got %d, want %d", pos, expect)
	}

	if rec != datum {
		t.Fatalf("unexpected record: got %d, want %d", rec, datum)
	}

	datum = 101
	if _, _, err = BinarySearch(ctx, j, 0, 100, fn); err != journal.ErrNotFound {
		t.Fatalf("unexpected error: got %d, want %d", err, journal.ErrNotFound)
	}
}
