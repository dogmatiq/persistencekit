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
		in.BeforeSet(func(string, []byte, []byte) error {
			return want
		})

		ks, err := store.Open(t.Context(), "<keyspace>")
		if err != nil {
			t.Fatal(err)
		}
		defer ks.Close()

		err = ks.Set(t.Context(), []byte("<key>"), []byte("<value>"))
		if err != want {
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
		in.AfterSet(func(string, []byte, []byte) error {
			return want
		})

		ks, err := store.Open(t.Context(), "<keyspace>")
		if err != nil {
			t.Fatal(err)
		}
		defer ks.Close()

		err = ks.Set(t.Context(), []byte("<key>"), []byte("<value>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		v, err := ks.Get(t.Context(), []byte("<key>"))
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(v, []byte("<value>")) {
			t.Fatalf("unexpected value: got %q, want %q", string(v), "<value>")
		}
	})

	t.Run("it allows functions to be cleared", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		in.BeforeOpen(func(string) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeSet(func(string, []byte, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.AfterSet(func(string, []byte, []byte) error {
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

		if err := ks.Set(t.Context(), []byte("<key>"), []byte("<value>")); err != nil {
			t.Fatal(err)
		}
	})
}
