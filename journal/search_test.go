package journal_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestBinarySearch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := &memoryjournal.Store{}
	j, err := store.Open(ctx, "test")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer j.Close()

	for pos := Position(0); pos < 100; pos++ {
		if err := j.Append(ctx, pos, []byte{byte(pos)}); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	datum := []byte{55}
	fn := func(
		ctx context.Context,
		pos Position,
		rec []byte,
	) (int, error) {
		return bytes.Compare(rec, datum), nil
	}

	pos, rec, err := BinarySearch(ctx, j, 0, 100, fn)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	expect := Position(55)
	if pos != expect {
		t.Fatalf("unexpected position: got %d, want %d", pos, expect)
	}

	if !bytes.Equal(rec, datum) {
		t.Fatalf("unexpected record: got %v, want %v", rec, datum)
	}

	datum = []byte{101}

	if _, _, err = BinarySearch(ctx, j, 0, 100, fn); err != ErrNotFound {
		t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
	}
}
