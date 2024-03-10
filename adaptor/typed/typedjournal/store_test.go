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

func TestStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := Store[int, typedmarshaler.JSON[int]]{
		Store: &memoryjournal.Store{},
	}

	j, err := store.Open(ctx, "<name>")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer j.Close()

	if err := j.Append(ctx, 0, 100); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := j.Append(ctx, 1, 101); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	fn := func(ctx context.Context, pos journal.Position, rec int) (bool, error) {
		expect := int(pos) + 100
		if rec != expect {
			t.Fatalf("unexpected value at position %d: got %d, want %d", pos, rec, expect)
		}
		return true, nil
	}

	if err := j.Range(ctx, 0, fn); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for pos := journal.Position(0); pos < 2; pos++ {
		rec, err := j.Get(ctx, pos)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fn(ctx, pos, rec)
	}
}
