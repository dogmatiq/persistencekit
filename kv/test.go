package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// RunTests runs tests that confirm a [BinaryStore] implementation behaves correctly.
func RunTests(
	t *testing.T,
	newStore func(t *testing.T) BinaryStore,
) {
	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows keyspaces to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				store := newStore(t)

				ks1, err := store.Open(ctx, "<keyspace>")
				if err != nil {
					t.Fatal(err)
				}
				defer ks1.Close()

				ks2, err := store.Open(ctx, "<keyspace>")
				if err != nil {
					t.Fatal(err)
				}
				defer ks2.Close()

				expect := []byte("<value>")
				if err := ks1.Set(ctx, []byte("<key>"), expect); err != nil {
					t.Fatal(err)
				}

				actual, err := ks2.Get(ctx, []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(expect, actual) {
					t.Fatalf(
						"unexpected record, want %q, got %q",
						string(expect),
						string(actual),
					)
				}
			})
		})
	})

	t.Run("Keyspace", func(t *testing.T) {
		t.Parallel()

		t.Run("Get", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns an empty value if the key doesn't exist", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				v, err := ks.Get(ctx, []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 0 {
					t.Fatal("expected zero-length value")
				}
			})

			t.Run("it returns an empty value if the key has been deleted", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")

				if err := ks.Set(ctx, k, []byte("<value>")); err != nil {
					t.Fatal(err)
				}

				if err := ks.Set(ctx, k, nil); err != nil {
					t.Fatal(err)
				}

				v, err := ks.Get(ctx, k)
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 0 {
					t.Fatal("expected zero-length value")
				}
			})

			t.Run("it returns the value if the key exists", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				for i := 0; i < 5; i++ {
					k := []byte(fmt.Sprintf("<key-%d>", i))
					v := []byte(fmt.Sprintf("<value-%d>", i))

					if err := ks.Set(ctx, k, v); err != nil {
						t.Fatal(err)
					}
				}

				for i := 0; i < 5; i++ {
					k := []byte(fmt.Sprintf("<key-%d>", i))
					expect := []byte(fmt.Sprintf("<value-%d>", i))

					actual, err := ks.Get(ctx, k)
					if err != nil {
						t.Fatal(err)
					}

					if !bytes.Equal(expect, actual) {
						t.Fatalf(
							"unexpected value, want %q, got %q",
							string(expect),
							string(actual),
						)
					}
				}
			})

			t.Run("it does not return its internal byte slice", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")

				if err := ks.Set(ctx, k, []byte("<value>")); err != nil {
					t.Fatal(err)
				}

				v, err := ks.Get(ctx, k)
				if err != nil {
					t.Fatal(err)
				}

				v[0] = 'X'

				actual, err := ks.Get(ctx, k)
				if err != nil {
					t.Fatal(err)
				}

				if expect := []byte("<value>"); !bytes.Equal(expect, actual) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expect),
						string(actual),
					)
				}
			})
		})

		t.Run("Set", func(t *testing.T) {
			t.Parallel()

			t.Run("it does not keep a reference to the key slice", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")
				v := []byte("<value>")

				if err := ks.Set(ctx, k, v); err != nil {
					t.Fatal(err)
				}

				k[0] = 'X'

				ok, err := ks.Has(ctx, k)
				if err != nil {
					t.Fatal(err)
				}

				if ok {
					t.Fatalf("unexpected key: %q", string(k))
				}

				actual, err := ks.Get(ctx, []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}

				if expect := []byte("<value>"); !bytes.Equal(expect, actual) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expect),
						string(actual),
					)
				}
			})

			t.Run("it does not keep a reference to the value slice", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")
				v := []byte("<value>")

				if err := ks.Set(ctx, k, v); err != nil {
					t.Fatal(err)
				}

				v[0] = 'X'

				actual, err := ks.Get(ctx, k)
				if err != nil {
					t.Fatal(err)
				}

				if expect := []byte("<value>"); !bytes.Equal(expect, actual) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expect),
						string(actual),
					)
				}
			})
		})

		t.Run("Has", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns false if the key doesn't exist", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				ok, err := ks.Has(ctx, []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})

			t.Run("it returns true if the key exists", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")

				if err := ks.Set(ctx, k, []byte("<value>")); err != nil {
					t.Fatal(err)
				}

				ok, err := ks.Has(ctx, k)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})

			t.Run("it returns false if the key has been deleted", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")

				if err := ks.Set(ctx, k, []byte("<value>")); err != nil {
					t.Fatal(err)
				}

				if err := ks.Set(ctx, k, nil); err != nil {
					t.Fatal(err)
				}

				ok, err := ks.Has(ctx, k)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})
		})

		t.Run("Range", func(t *testing.T) {
			t.Parallel()

			t.Run("calls the function for each key in the keyspace", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				expect := map[string]string{}

				for n := uint64(0); n < 100; n++ {
					k := fmt.Sprintf("<key-%d>", n)
					v := fmt.Sprintf("<value-%d>", n)
					if err := ks.Set(ctx, []byte(k), []byte(v)); err != nil {
						t.Fatal(err)
					}

					expect[k] = v
				}

				actual := map[string]string{}

				if err := ks.Range(
					ctx,
					func(ctx context.Context, k, v []byte) (bool, error) {
						actual[string(k)] = string(v)
						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				if diff := cmp.Diff(expect, actual); diff != "" {
					t.Fatal(diff)
				}
			})

			t.Run("it stops iterating if the function returns false", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				for n := uint64(0); n < 2; n++ {
					k := fmt.Sprintf("<key-%d>", n)
					v := fmt.Sprintf("<value-%d>", n)
					if err := ks.Set(ctx, []byte(k), []byte(v)); err != nil {
						t.Fatal(err)
					}
				}

				called := false
				if err := ks.Range(
					ctx,
					func(ctx context.Context, k, v []byte) (bool, error) {
						if called {
							return false, errors.New("unexpected call")
						}

						called = true
						return false, nil
					},
				); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it does not invoke the function with its internal byte slices", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				if err := ks.Set(
					ctx,
					[]byte("<key>"),
					[]byte("<value>"),
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					ctx,
					func(ctx context.Context, k, v []byte) (bool, error) {
						k[0] = 'X'
						v[0] = 'Y'

						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				k := []byte("Xkey>")

				ok, err := ks.Has(ctx, k)
				if err != nil {
					t.Fatal(err)
				}

				if ok {
					t.Fatalf("unexpected key: %q", string(k))
				}

				actual, err := ks.Get(ctx, []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}

				if expect := []byte("<value>"); !bytes.Equal(expect, actual) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expect),
						string(actual),
					)
				}
			})

			t.Run("it allows calls to Get() during iteration", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				if err := ks.Set(
					ctx,
					[]byte("<key>"),
					[]byte("<value>"),
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					ctx,
					func(ctx context.Context, k, expect []byte) (bool, error) {
						actual, err := ks.Get(ctx, k)
						if err != nil {
							t.Fatal(err)
						}

						if !bytes.Equal(expect, actual) {
							t.Fatalf(
								"unexpected value, want %q, got %q",
								string(expect),
								string(actual),
							)
						}

						return false, nil
					},
				); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it allows calls to Has() during iteration", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				if err := ks.Set(
					ctx,
					[]byte("<key>"),
					[]byte("<value>"),
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					ctx,
					func(ctx context.Context, k, _ []byte) (bool, error) {
						ok, err := ks.Has(ctx, k)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							t.Fatal("expected key to exist")
						}
						return false, nil
					},
				); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it allows calls to Set() during iteration", func(t *testing.T) {
				t.Parallel()

				ctx, ks := setup(t, newStore)

				k := []byte("<key>")

				if err := ks.Set(
					ctx,
					k,
					[]byte("<value>"),
				); err != nil {
					t.Fatal(err)
				}

				expect := []byte("<updated>")

				if err := ks.Range(
					ctx,
					func(ctx context.Context, k, _ []byte) (bool, error) {
						if err := ks.Set(ctx, k, expect); err != nil {
							t.Fatal(err)
						}
						return false, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				actual, err := ks.Get(ctx, k)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(expect, actual) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expect),
						string(actual),
					)
				}
			})
		})
	})
}

var nameCounter atomic.Uint64

func uniqueName() string {
	return fmt.Sprintf("<keyspace-%d>", nameCounter.Add(1))
}

func setup(
	t *testing.T,
	newStore func(t *testing.T) BinaryStore,
) (context.Context, BinaryKeyspace) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	store := newStore(t)

	ks, err := store.Open(ctx, uniqueName())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := ks.Close(); err != nil {
			t.Error(err)
		}
	})

	return ctx, ks
}
