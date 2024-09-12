package journal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestAppendWithConflictResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := &memoryjournal.Store[int]{}
	j, err := store.Open(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("it appends records", func(t *testing.T) {
		end, err := AppendWithConflictResolution(
			ctx,
			j,
			0,
			100,
			func(context.Context, Position) (Position, error) {
				t.Fatal("unexpected call")
				return 0, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		expectEnd := Position(1)
		expectRec := 100

		if end != expectEnd {
			t.Fatalf("unexpected end position: got %d, want %d", end, expectEnd)
		}

		rec, err := j.Get(ctx, end-1)
		if err != nil {
			t.Fatal(err)
		}

		if rec != expectRec {
			t.Fatalf("unexpected record: got %d, want %d", rec, expectRec)
		}
	})

	t.Run("it retries on conflict", func(t *testing.T) {
		if err := j.Append(ctx, 1, 200); err != nil {
			t.Fatal(err)
		}

		end, err := AppendWithConflictResolution(
			ctx,
			j,
			0,
			300,
			func(ctx context.Context, pos Position) (Position, error) {
				return pos + 1, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		expectEnd := Position(3)
		expectRec := 300

		if end != expectEnd {
			t.Fatalf("unexpected end position: got %d, want %d", end, expectEnd)
		}

		rec, err := j.Get(ctx, end-1)
		if err != nil {
			t.Fatal(err)
		}

		if rec != expectRec {
			t.Fatalf("unexpected record: got %d, want %d", rec, expectRec)
		}
	})

	t.Run("it returns the error returned by the conflict resolution function", func(t *testing.T) {
		expectErr := errors.New("<error>")

		if _, err := AppendWithConflictResolution(
			ctx,
			j,
			0,
			400,
			func(context.Context, Position) (Position, error) {
				return 0, expectErr
			},
		); err != expectErr {
			t.Fatalf("unexpected error: got %v, want %v", err, expectErr)
		}
	})
}
