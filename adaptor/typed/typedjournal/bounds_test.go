package typedjournal_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/adaptor/typed/typedjournal"
	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
)

func TestIsFresh(t *testing.T) {
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

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns true", func(t *testing.T) {
			ok, err := IsFresh(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(ctx, 0, 100); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			ok, err := IsFresh(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(ctx, 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it continues to return false", func(t *testing.T) {
			ok, err := IsFresh(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})
}

func TestIsEmpty(t *testing.T) {
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

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns true", func(t *testing.T) {
			ok, err := IsEmpty(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(ctx, 0, 100); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			ok, err := IsEmpty(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(ctx, 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns true", func(t *testing.T) {
			ok, err := IsEmpty(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
		})
	})
}

func TestFirstRecord(t *testing.T) {
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

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := FirstRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(ctx, 0, 100); err != nil {
			t.Fatal(err)
		}

		if err := j.Append(ctx, 1, 200); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns the first record", func(t *testing.T) {
			pos, rec, ok, err := FirstRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
			if pos != 0 {
				t.Fatalf("unexpected position: got %d, want %d", pos, 0)
			}

			expect := 100
			if rec != expect {
				t.Fatalf("unexpected record: got %q, want %q", rec, expect)
			}
		})
	})

	t.Run("when the journal has been partially truncated", func(t *testing.T) {
		if err := j.Truncate(ctx, 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns the first non-truncated record", func(t *testing.T) {
			pos, rec, ok, err := FirstRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
			if pos != 1 {
				t.Fatalf("unexpected position: got %d, want %d", pos, 1)
			}

			expect := 200
			if rec != expect {
				t.Fatalf("unexpected record: got %q, want %q", rec, expect)
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(ctx, 2); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := FirstRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})
}

func TestLastRecord(t *testing.T) {
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

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := LastRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(ctx, 0, 100); err != nil {
			t.Fatal(err)
		}

		if err := j.Append(ctx, 1, 200); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns the last record", func(t *testing.T) {
			pos, rec, ok, err := LastRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
			if pos != 1 {
				t.Fatalf("unexpected position: got %d, want %d", pos, 1)
			}

			expect := 200
			if rec != expect {
				t.Fatalf("unexpected record: got %q, want %q", rec, expect)
			}
		})
	})

	t.Run("when the journal has been partially truncated", func(t *testing.T) {
		if err := j.Truncate(ctx, 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it continues to return the last record", func(t *testing.T) {
			pos, rec, ok, err := LastRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
			if pos != 1 {
				t.Fatalf("unexpected position: got %d, want %d", pos, 1)
			}

			expect := 200
			if rec != expect {
				t.Fatalf("unexpected record: got %q, want %q", rec, expect)
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(ctx, 2); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := LastRecord(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})
}
