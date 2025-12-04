package journal_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
	. "github.com/dogmatiq/persistencekit/journal"
)

func TestWithInterceptor(t *testing.T) {
	t.Parallel()

	setup := func() (BinaryStore, *BinaryInterceptor) {
		var in BinaryInterceptor
		return WithInterceptor(&memoryjournal.BinaryStore{}, &in), &in
	}

	journal.RunTests(
		t,
		WithInterceptor(
			&memoryjournal.BinaryStore{},
			&journal.Interceptor[[]byte]{},
		),
	)

	t.Run("it returns the given store if no interceptor is provided", func(t *testing.T) {
		t.Parallel()

		underlying := &memoryjournal.BinaryStore{}
		store := WithInterceptor(underlying, nil)

		if store != underlying {
			t.Fatalf("unexpected store: got %T, want %T", store, underlying)
		}
	})

	t.Run("it invokes the BeforeOpen function", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.BeforeOpen(func(name string) error {
			return want
		})

		_, got := store.Open(t.Context(), "<journal>")
		if got != want {
			t.Fatalf("unexpected error: got %v, want %v", got, want)
		}
	})

	t.Run("it invokes the BeforeAppend function", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.BeforeAppend(func(string, []byte) error {
			return want
		})

		j, err := store.Open(t.Context(), "<journal>")
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()

		err = j.Append(t.Context(), 0, []byte("<record>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		if _, err := j.Get(t.Context(), 0); !journal.IsNotFound(err) {
			t.Fatalf("unexpected error: got %v, want IsNotFound(err) == true", err)
		}
	})

	t.Run("it invokes the AfterAppend function", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.AfterAppend(func(string, []byte) error {
			return want
		})

		j, err := store.Open(t.Context(), "<journal>")
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()

		err = j.Append(t.Context(), 0, []byte("<record>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		rec, err := j.Get(t.Context(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(rec, []byte("<record>")) {
			t.Fatalf("unexpected record: got %q, want %q", string(rec), "<record>")
		}
	})

	t.Run("it allows functions to be cleared", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		in.BeforeOpen(func(string) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeAppend(func(string, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.AfterAppend(func(string, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeOpen(nil)
		in.BeforeAppend(nil)
		in.AfterAppend(nil)

		j, err := store.Open(t.Context(), "<journal>")
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()

		if err := j.Append(t.Context(), 0, []byte("<record>")); err != nil {
			t.Fatal(err)
		}
	})
}
