package journal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestSearch(t *testing.T) {
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	for i := range 100 {
		if err := j.Append(t.Context(), Position(i), i); err != nil {
			t.Fatal(err)
		}
	}

	datum := 55
	cmp := func(
		_ context.Context,
		_ Position,
		rec int,
	) (int, error) {
		return rec - datum, nil
	}

	pos, rec, err := Search(t.Context(), j, Interval{0, 100}, cmp)
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
	if _, _, err := Search(t.Context(), j, Interval{0, 100}, cmp); !IsNotFound(err) {
		t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
	}
}

func TestRangeFromSearchResult(t *testing.T) {
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	for i := range 100 {
		if err := j.Append(t.Context(), Position(i), i); err != nil {
			t.Fatal(err)
		}
	}

	datum := 55
	cmp := func(
		_ context.Context,
		_ Position,
		rec int,
	) (int, error) {
		return rec - datum, nil
	}

	t.Run("it calls the function with the search result", func(t *testing.T) {
		called := false

		fn := func(
			_ context.Context,
			pos Position,
			rec int,
		) (bool, error) {
			called = true

			if pos != Position(datum) {
				t.Fatalf("unexpected position: got %d, want %d", pos, datum)
			}
			if rec != datum {
				t.Fatalf("unexpected record: got %d, want %d", rec, datum)
			}

			return false, nil
		}

		if err := RangeFromSearchResult(t.Context(), j, Interval{0, 100}, cmp, fn); err != nil {
			t.Fatal(err)
		}

		if !called {
			t.Fatal("the function was not called")
		}
	})

	t.Run("it calls the function with the records after the search result", func(t *testing.T) {
		want := datum
		calls := 0

		fn := func(
			_ context.Context,
			pos Position,
			rec int,
		) (bool, error) {
			calls++

			if pos != Position(want) {
				t.Fatalf("unexpected position: got %d, want %d", pos, want)
			}
			if rec != want {
				t.Fatalf("unexpected record: got %d, want %d", rec, want)
			}

			if rec == 59 {
				return false, nil
			}

			want++

			return true, nil
		}

		if err := RangeFromSearchResult(t.Context(), j, Interval{0, 100}, cmp, fn); err != nil {
			t.Fatal(err)
		}

		wantCalls := 5 // 55 (datum), 56, 57, 58, 59
		if calls != wantCalls {
			t.Fatalf("unexpected number of calls: got %d, want %d", calls, wantCalls)
		}
	})

	t.Run("it returns a not found error if the search result is not found", func(t *testing.T) {
		datum = 101

		fn := func(
			_ context.Context,
			pos Position,
			rec int,
		) (bool, error) {
			return false, errors.New("unexpected call")
		}

		if err := RangeFromSearchResult(t.Context(), j, Interval{0, 100}, cmp, fn); !IsNotFound(err) {
			t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
		}
	})

}
