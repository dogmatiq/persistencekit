package journal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestAppendWithConflictResolution(t *testing.T) {
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("it appends records", func(t *testing.T) {
		end, err := AppendWithConflictResolution(
			t.Context(),
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

		rec, err := j.Get(t.Context(), end-1)
		if err != nil {
			t.Fatal(err)
		}

		if rec != expectRec {
			t.Fatalf("unexpected record: got %d, want %d", rec, expectRec)
		}
	})

	t.Run("it retries on conflict", func(t *testing.T) {
		if err := j.Append(t.Context(), 1, 200); err != nil {
			t.Fatal(err)
		}

		end, err := AppendWithConflictResolution(
			t.Context(),
			j,
			0,
			300,
			func(_ context.Context, pos Position) (Position, error) {
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

		rec, err := j.Get(t.Context(), end-1)
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
			t.Context(),
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
