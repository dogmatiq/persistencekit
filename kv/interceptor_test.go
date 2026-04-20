package kv_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/kv"
	. "github.com/dogmatiq/persistencekit/kv"
)

func TestWithInterceptor(t *testing.T) {
	t.Parallel()

	setup := func() (BinaryStore, *BinaryInterceptor) {
		var in BinaryInterceptor
		return WithInterceptor(&memorykv.BinaryStore{}, &in), &in
	}

	kv.RunTests(
		t,
		WithInterceptor(
			&memorykv.BinaryStore{},
			&kv.Interceptor[[]byte, []byte]{},
		),
	)

	t.Run("it returns the given store if no interceptor is provided", func(t *testing.T) {
		t.Parallel()

		underlying := &memorykv.BinaryStore{}
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

		_, got := store.Open(t.Context(), "<keyspace>")
		if got != want {
			t.Fatalf("unexpected error: got %v, want %v", got, want)
		}
	})

	t.Run("it invokes the BeforeSet function", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.BeforeSet(func(_ string, _, _ []byte, _ *Revision) error {
			return want
		})

		ks, err := store.Open(t.Context(), "<keyspace>")
		if err != nil {
			t.Fatal(err)
		}
		defer ks.Close()

		if _, err := ks.Set(t.Context(), []byte("<key>"), []byte("<value>"), ""); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		ok, err := ks.Has(t.Context(), []byte("<key>"))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("did not expect key to be set")
		}
	})

	t.Run("it invokes the AfterSet function", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.AfterSet(func(_ string, _, _ []byte, _ *Revision) error {
			return want
		})

		ks, err := store.Open(t.Context(), "<keyspace>")
		if err != nil {
			t.Fatal(err)
		}
		defer ks.Close()

		if _, err := ks.Set(t.Context(), []byte("<key>"), []byte("<value>"), ""); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		v, _, err := ks.Get(t.Context(), []byte("<key>"))
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(v, []byte("<value>")) {
			t.Fatalf("unexpected value: got %q, want %q", string(v), "<value>")
		}
	})

	t.Run("it allows BeforeSet to modify the revision and passes the result to AfterSet", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		in.BeforeSet(func(_ string, _, _ []byte, r *Revision) error {
			*r = "" // force an unconditional insert regardless of what the caller passes
			return nil
		})

		var got Revision
		in.AfterSet(func(_ string, _, _ []byte, r *Revision) error {
			got = *r
			return nil
		})

		ks, err := store.Open(t.Context(), "<keyspace>")
		if err != nil {
			t.Fatal(err)
		}
		defer ks.Close()

		// Seed one entry so we can observe whether the revision is actually used.
		if _, err := ks.Set(t.Context(), []byte("<key>"), []byte("<value>"), ""); err != nil {
			t.Fatal(err)
		}

		// Pass a bogus non-empty revision; BeforeSet will replace it with ""
		// (insert), so the call should succeed rather than conflict.
		if _, err := ks.Set(t.Context(), []byte("<key2>"), []byte("<value>"), "bogus"); err != nil {
			t.Fatal(err)
		}

		if got == "" {
			t.Fatal("expected AfterSet to receive a non-empty revision")
		}
	})

	t.Run("it allows functions to be cleared", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		in.BeforeOpen(func(string) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeSet(func(_ string, _, _ []byte, _ *Revision) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.AfterSet(func(_ string, _, _ []byte, _ *Revision) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeOpen(nil)
		in.BeforeSet(nil)
		in.AfterSet(nil)

		ks, err := store.Open(t.Context(), "<keyspace>")
		if err != nil {
			t.Fatal(err)
		}
		defer ks.Close()

		if _, err := ks.Set(t.Context(), []byte("<key>"), []byte("<value>"), ""); err != nil {
			t.Fatal(err)
		}
	})
}
