package journal_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestIsFresh(t *testing.T) {
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns true", func(t *testing.T) {
			ok, err := IsFresh(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(t.Context(), 0, 100); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			ok, err := IsFresh(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(t.Context(), 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it continues to return false", func(t *testing.T) {
			ok, err := IsFresh(t.Context(), j)
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
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns true", func(t *testing.T) {
			ok, err := IsEmpty(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected ok to be true")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(t.Context(), 0, 100); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			ok, err := IsEmpty(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(t.Context(), 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns true", func(t *testing.T) {
			ok, err := IsEmpty(t.Context(), j)
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
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := FirstRecord(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(t.Context(), 0, 100); err != nil {
			t.Fatal(err)
		}

		if err := j.Append(t.Context(), 1, 200); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns the first record", func(t *testing.T) {
			pos, rec, ok, err := FirstRecord(t.Context(), j)
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
				t.Fatalf("unexpected record: got %d, want %d", rec, expect)
			}
		})
	})

	t.Run("when the journal has been partially truncated", func(t *testing.T) {
		if err := j.Truncate(t.Context(), 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns the first non-truncated record", func(t *testing.T) {
			pos, rec, ok, err := FirstRecord(t.Context(), j)
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
				t.Fatalf("unexpected record: got %d, want %d", rec, expect)
			}
		})
	})

	t.Run("when the journal has been fully truncated", func(t *testing.T) {
		if err := j.Truncate(t.Context(), 2); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := FirstRecord(t.Context(), j)
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
	store := &memoryjournal.Store[int]{}
	j, err := store.Open(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	t.Run("when the journal is empty", func(t *testing.T) {
		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := LastRecord(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})

	t.Run("when the journal has records", func(t *testing.T) {
		if err := j.Append(t.Context(), 0, 100); err != nil {
			t.Fatal(err)
		}

		if err := j.Append(t.Context(), 1, 200); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns the last record", func(t *testing.T) {
			pos, rec, ok, err := LastRecord(t.Context(), j)
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
				t.Fatalf("unexpected record: got %d, want %d", rec, expect)
			}
		})
	})

	t.Run("when the journal has been partially truncated", func(t *testing.T) {
		if err := j.Truncate(t.Context(), 1); err != nil {
			t.Fatal(err)
		}

		t.Run("it continues to return the last record", func(t *testing.T) {
			pos, rec, ok, err := LastRecord(t.Context(), j)
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
		if err := j.Truncate(t.Context(), 2); err != nil {
			t.Fatal(err)
		}

		t.Run("it returns false", func(t *testing.T) {
			_, _, ok, err := LastRecord(t.Context(), j)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected ok to be false")
			}
		})
	})
}
