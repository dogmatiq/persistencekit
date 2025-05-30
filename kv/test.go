package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/internal/testx"
	"github.com/google/go-cmp/cmp"
	"pgregory.net/rapid"
)

// RunTests runs tests that confirm a [BinaryStore] implementation behaves correctly.
func RunTests(
	t *testing.T,
	store BinaryStore,
) {
	setup := func(t *testing.T) (context.Context, BinaryKeyspace) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)

		name := testx.SequentialName("keyspace")

		ks, err := store.Open(ctx, name)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := ks.Close(); err != nil {
				t.Error(err)
			}
		})

		if ks.Name() != name {
			t.Fatalf("unexpected keyspace name: got %q, want %q", ks.Name(), name)
		}

		return ctx, ks
	}

	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows keyspaces to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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
					func(_ context.Context, k, v []byte) (bool, error) {
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

				ctx, ks := setup(t)

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
					func(_ context.Context, _, _ []byte) (bool, error) {
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

				ctx, ks := setup(t)

				if err := ks.Set(
					ctx,
					[]byte("<key>"),
					[]byte("<value>"),
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					ctx,
					func(_ context.Context, k, v []byte) (bool, error) {
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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

				ctx, ks := setup(t)

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

	t.Run("property-based", func(t *testing.T) {
		t.Parallel()

		rapid.Check(t, func(t *rapid.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			ks, err := store.Open(ctx, testx.SequentialName("keyspace"))
			if err != nil {
				t.Fatal(err)
			}
			defer ks.Close()

			nonEmptyValue := rapid.StringN(1, -1, -1)

			pairs := map[string][]byte{}
			var keys [][]byte

			t.Repeat(
				map[string]func(*rapid.T){
					"Get": func(t *rapid.T) {
						key := []byte(nonEmptyValue.Draw(t, "key"))

						value, err := ks.Get(ctx, key)
						if err != nil {
							t.Fatal(err)
						}

						expect := pairs[string(key)]
						if !bytes.Equal(expect, value) {
							t.Fatalf(
								"unexpected value for key %q: got %q, want %q",
								string(key),
								string(value),
								string(expect),
							)
						}
					},
					"Get (key exists)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")

						value, err := ks.Get(ctx, key)
						if err != nil {
							t.Fatal(err)
						}

						expect := pairs[string(key)]
						if !bytes.Equal(expect, value) {
							t.Fatalf(
								"unexpected value for key %q: got %q, want %q",
								string(key),
								string(value),
								string(expect),
							)
						}
					},
					"Has": func(t *rapid.T) {
						key := []byte(nonEmptyValue.Draw(t, "key"))

						ok, err := ks.Has(ctx, key)
						if err != nil {
							t.Fatal(err)
						}

						_, expect := pairs[string(key)]
						if ok != expect {
							t.Fatalf(
								"unexpected has for key %q: got %t, want %t",
								string(key),
								ok,
								expect,
							)
						}
					},
					"Has (key exists)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")

						ok, err := ks.Has(ctx, key)
						if err != nil {
							t.Fatal(err)
						}

						expect := true
						if ok != expect {
							t.Fatalf(
								"unexpected has for key %q: got %t, want %t",
								string(key),
								ok,
								expect,
							)
						}
					},
					"Set": func(t *rapid.T) {
						key := []byte(nonEmptyValue.Draw(t, "key"))
						value := []byte(nonEmptyValue.Draw(t, "value"))

						if err := ks.Set(ctx, key, value); err != nil {
							t.Fatal(err)
						}

						n := len(pairs)
						pairs[string(key)] = value
						if len(pairs) > n {
							keys = append(keys, key)
						}
					},
					"Set (replace)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")
						value := []byte(nonEmptyValue.Draw(t, "value"))

						if err := ks.Set(ctx, key, value); err != nil {
							t.Fatal(err)
						}

						pairs[string(key)] = value
					},
					"Set (delete)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")

						if err := ks.Set(ctx, key, nil); err != nil {
							t.Fatal(err)
						}

						delete(pairs, string(key))
						keys = slices.DeleteFunc(
							keys,
							func(k []byte) bool {
								return bytes.Equal(k, key)
							},
						)
					},
					"Range": func(t *rapid.T) {
						seen := map[string]struct{}{}

						if err := ks.Range(
							ctx,
							func(_ context.Context, k, v []byte) (bool, error) {
								if _, ok := seen[string(k)]; ok {
									t.Fatalf(
										"key seen twice while ranging over pairs: %q",
										string(k),
									)
								}
								seen[string(k)] = struct{}{}

								expect := pairs[string(k)]
								if !bytes.Equal(expect, v) {
									t.Fatalf(
										"unexpected value for key %q: got %q, want %q",
										string(k),
										string(v),
										string(expect),
									)
								}

								return true, nil
							},
						); err != nil {
							t.Fatal(err)
						}

						for key := range pairs {
							if _, ok := seen[key]; !ok {
								t.Fatalf("key not seen while ranging over pairs: %q", key)
							}
						}
					},
				},
			)
		})
	})
}
