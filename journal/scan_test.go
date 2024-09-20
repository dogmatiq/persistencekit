package journal_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := &memoryjournal.Store[int]{}
	j, err := store.Open(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns a not found error", func(t *testing.T) {
			if _, err := Scan(
				ctx,
				j,
				0,
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					t.Fatal("unexpected call")
					return 0, false, nil
				},
			); !IsNotFound(err) {
				t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			if err := j.Append(ctx, Position(i), 100*(i+1)); err != nil {
				t.Fatal(err)
			}
		}

		t.Run("it returns the value from the first matching predicate call", func(t *testing.T) {
			v, err := Scan(
				ctx,
				j,
				0,
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					return rec * 10, rec > 100, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			expect := 2000
			if v != expect {
				t.Fatalf("unexpected value: got %d, want %d", v, expect)
			}
		})

		t.Run("it returns a ValueNotFoundError if no predicate call returns true", func(t *testing.T) {
			if _, err := Scan(
				ctx,
				j,
				0,
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					return 0, false, nil
				},
			); !IsNotFound(err) {
				t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
			}
		})

		t.Run("starts at the given position", func(t *testing.T) {
			v, err := Scan(
				ctx,
				j,
				1,
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					return rec, true, nil
				},
			)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			expect := 200
			if v != expect {
				t.Fatalf("unexpected value: got %d, want %d", v, expect)
			}
		})
	})
}

func TestScanFromSearchResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := &memoryjournal.Store[int]{}
	j, err := store.Open(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns a ValueNotFoundError", func(t *testing.T) {
			if _, err := ScanFromSearchResult(
				ctx,
				j,
				Interval{0, 0},
				func(ctx context.Context, pos Position, rec int) (cmp int, err error) {
					t.Fatal("unexpected call")
					return 0, nil
				},
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					t.Fatal("unexpected call")
					return 0, false, nil
				},
			); !IsNotFound(err) {
				t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			if err := j.Append(ctx, Position(i), 100*(i+1)); err != nil {
				t.Fatal(err)
			}
		}

		t.Run("it returns the value when the predicate matches the result of the binary search", func(t *testing.T) {
			v, err := ScanFromSearchResult(
				ctx,
				j,
				Interval{0, 10},
				func(ctx context.Context, pos Position, rec int) (cmp int, err error) {
					return rec - 300, nil
				},
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					return rec * 10, rec == 300, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			expect := 3000
			if v != expect {
				t.Fatalf("unexpected value: got %d, want %d", v, expect)
			}
		})

		t.Run("it returns the value from the first matching predicate after the result of the binary search", func(t *testing.T) {
			v, err := ScanFromSearchResult(
				ctx,
				j,
				Interval{0, 10},
				func(ctx context.Context, pos Position, rec int) (cmp int, err error) {
					// Search for the record with value 300.
					return rec - 300, nil
				},
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					// Then find the first record with a value that is a multiple of 200.
					return rec, rec%200 == 0, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			expect := 400
			if v != expect {
				t.Fatalf("unexpected value: got %d, want %d", v, expect)
			}
		})

		t.Run("it returns a ValueNotFoundError if the binary search produces no result", func(t *testing.T) {
			if _, err := ScanFromSearchResult(
				ctx,
				j,
				Interval{
					4, // exclude the record with value 300 from the search range
					10,
				},
				func(ctx context.Context, pos Position, rec int) (cmp int, err error) {
					// Search for the record with value 300.
					return rec - 300, nil
				},
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					// Then find the first record with a value that is a multiple of 200.
					return rec, rec%200 == 0, nil
				},
			); !IsNotFound(err) {
				t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
			}
		})

		t.Run("it returns a ValueNotFoundError if the scan produces no result", func(t *testing.T) {
			if _, err := ScanFromSearchResult(
				ctx,
				j,
				Interval{0, 10},
				func(ctx context.Context, pos Position, rec int) (cmp int, err error) {
					return rec - 300, nil
				},
				func(ctx context.Context, pos Position, rec int) (int, bool, error) {
					return 0, false, nil
				},
			); !IsNotFound(err) {
				t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
			}
		})
	})
}
