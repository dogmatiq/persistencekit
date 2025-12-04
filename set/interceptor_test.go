package set_test

import (
	"errors"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/set"
	. "github.com/dogmatiq/persistencekit/set"
)

func TestWithInterceptor(t *testing.T) {
	t.Parallel()

	setup := func() (BinaryStore, *BinaryInterceptor) {
		var in BinaryInterceptor
		return WithInterceptor(&memoryset.BinaryStore{}, &in), &in
	}

	RunTests(
		t,
		WithInterceptor(
			&memoryset.BinaryStore{},
			&set.Interceptor[[]byte]{},
		),
	)

	t.Run("it returns the given store if no interceptor is provided", func(t *testing.T) {
		t.Parallel()

		underlying := &memoryset.BinaryStore{}
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

		_, got := store.Open(t.Context(), "<set>")
		if got != want {
			t.Fatalf("unexpected error: got %v, want %v", got, want)
		}
	})

	t.Run("it invokes the BeforeAdd function when using Add()", func(t *testing.T) {
		t.Parallel()

		want := errors.New("<error>")
		store, in := setup()

		in.BeforeAdd(func(set string, v []byte) error {
			return want
		})

		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}

		err = set.Add(t.Context(), []byte("<member>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		ok, err := set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}

		if ok {
			t.Fatal("did not expect member to be added")
		}
	})

	t.Run("it invokes the BeforeAdd function when using TryAdd()", func(t *testing.T) {
		t.Parallel()

		want := errors.New("<error>")
		store, in := setup()

		in.BeforeAdd(func(string, []byte) error {
			return want
		})

		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		ok, err := set.TryAdd(t.Context(), []byte("<member>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}
		if ok {
			t.Fatal("did not expect member to be added")
		}

		ok, err = set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("did not expect member to be present")
		}
	})

	t.Run("it invokes the AfterAdd function when using Add()", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.AfterAdd(func(string, []byte) error {
			return want
		})

		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		got := set.Add(t.Context(), []byte("<member>"))
		if got != want {
			t.Fatalf("unexpected error: got %v, want %v", got, want)
		}

		ok, err := set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(got)
		}
		if !ok {
			t.Fatal("expected member to be added")
		}
	})

	t.Run("it invokes the AfterAdd function when using TryAdd()", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		want := errors.New("<error>")
		in.AfterAdd(func(string, []byte) error {
			return want
		})

		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		ok, err := set.TryAdd(t.Context(), []byte("<member>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}
		if ok {
			t.Fatal("did not expect TryAdd() to report success")
		}

		ok, err = set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected member to be added")
		}
	})

	t.Run("it invokes the BeforeRemove function when using Remove()", func(t *testing.T) {
		t.Parallel()

		store, in := setup()
		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		if err := set.Add(t.Context(), []byte("<member>")); err != nil {
			t.Fatal(err)
		}

		want := errors.New("<error>")
		in.BeforeRemove(func(string, []byte) error {
			return want
		})

		if err := set.Remove(t.Context(), []byte("<member>")); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		ok, err := set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected member to remain")
		}
	})

	t.Run("it invokes the BeforeRemove function when using TryRemove()", func(t *testing.T) {
		t.Parallel()

		store, in := setup()
		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		if err := set.Add(t.Context(), []byte("<member>")); err != nil {
			t.Fatal(err)
		}

		want := errors.New("<error>")
		in.BeforeRemove(func(string, []byte) error {
			return want
		})

		removed, err := set.TryRemove(t.Context(), []byte("<member>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}
		if removed {
			t.Fatal("did not expect member to be removed")
		}

		ok, err := set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected member to remain")
		}
	})

	t.Run("it invokes the AfterRemove function when using Remove()", func(t *testing.T) {
		t.Parallel()

		store, in := setup()
		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		if err := set.Add(t.Context(), []byte("<member>")); err != nil {
			t.Fatal(err)
		}

		want := errors.New("<error>")
		in.AfterRemove(func(string, []byte) error {
			return want
		})

		if err := set.Remove(t.Context(), []byte("<member>")); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		ok, err := set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected member to be removed")
		}
	})

	t.Run("it invokes the AfterRemove function when using TryRemove()", func(t *testing.T) {
		t.Parallel()

		store, in := setup()
		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		if err := set.Add(t.Context(), []byte("<member>")); err != nil {
			t.Fatal(err)
		}

		want := errors.New("<error>")
		in.AfterRemove(func(string, []byte) error {
			return want
		})

		removed, err := set.TryRemove(t.Context(), []byte("<member>"))
		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}
		if removed {
			t.Fatal("did not expect TryRemove() to report success")
		}

		ok, err := set.Has(t.Context(), []byte("<member>"))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected member to be removed")
		}
	})

	t.Run("it allows functions to be cleared", func(t *testing.T) {
		t.Parallel()

		store, in := setup()

		in.BeforeOpen(func(string) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeAdd(func(string, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.AfterAdd(func(string, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeRemove(func(string, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.AfterRemove(func(string, []byte) error {
			t.Fatal("unexpected call")
			return nil
		})

		in.BeforeOpen(nil)
		in.BeforeAdd(nil)
		in.AfterAdd(nil)
		in.BeforeRemove(nil)
		in.AfterRemove(nil)

		set, err := store.Open(t.Context(), "<set>")
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()

		if err := set.Add(t.Context(), []byte("<member>")); err != nil {
			t.Fatal(err)
		}

		if err := set.Remove(t.Context(), []byte("<member>")); err != nil {
			t.Fatal(err)
		}
	})
}
