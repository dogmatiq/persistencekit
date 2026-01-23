package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
	"github.com/google/go-cmp/cmp"
	"pgregory.net/rapid"
)

// RunTests runs tests that confirm a [BinaryStore] implementation behaves correctly.
func RunTests(
	t *testing.T,
	store BinaryStore,
) {
	setup := func(t *testing.T) BinaryKeyspace {
		name := xtesting.SequentialName("keyspace")

		ks, err := store.Open(t.Context(), name)
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

		return ks
	}

	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows keyspaces to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				ks1, err := store.Open(t.Context(), "<keyspace>")
				if err != nil {
					t.Fatal(err)
				}
				defer ks1.Close()

				ks2, err := store.Open(t.Context(), "<keyspace>")
				if err != nil {
					t.Fatal(err)
				}
				defer ks2.Close()

				expect := []byte("<value>")
				if err := ks1.Set(t.Context(), []byte("<key>"), expect, 0); err != nil {
					t.Fatal(err)
				}

				actual, _, err := ks2.Get(t.Context(), []byte("<key>"))
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

				ks := setup(t)

				v, r, err := ks.Get(t.Context(), []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 0 {
					t.Fatal("expected zero-length value")
				}
				if r != 0 {
					t.Fatal("expected zero revision")
				}
			})

			t.Run("it returns an empty value if the key has been deleted", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")

				if err := ks.Set(t.Context(), k, []byte("<value>"), 0); err != nil {
					t.Fatal(err)
				}

				if err := ks.Set(t.Context(), k, nil, 1); err != nil {
					t.Fatal(err)
				}

				v, r, err := ks.Get(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 0 {
					t.Fatal("expected zero-length value")
				}
				if r != 0 {
					t.Fatal("expected zero revision")
				}
			})

			t.Run("it returns the value if the key exists", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				for i := range 5 {
					k := []byte(fmt.Sprintf("<key-%d>", i))
					v := []byte(fmt.Sprintf("<value-%d>", i))

					if err := ks.Set(t.Context(), k, v, 0); err != nil {
						t.Fatal(err)
					}
				}

				for i := range 5 {
					k := []byte(fmt.Sprintf("<key-%d>", i))
					expect := []byte(fmt.Sprintf("<value-%d>", i))

					actual, _, err := ks.Get(t.Context(), k)
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

				ks := setup(t)

				k := []byte("<key>")

				if err := ks.Set(t.Context(), k, []byte("<value>"), 0); err != nil {
					t.Fatal(err)
				}

				v, _, err := ks.Get(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}

				v[0] = 'X'

				actual, _, err := ks.Get(t.Context(), k)
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

			t.Run("when the key is present in the keyspace", func(t *testing.T) {
				t.Run("it returns a ConflictError if the given revision does not match", func(t *testing.T) {
					cases := []struct {
						Name     string
						Revision Revision
					}{
						{"too low", 0},
						{"too high", 3},
					}

					for _, c := range cases {
						t.Run(c.Name, func(t *testing.T) {
							t.Parallel()

							ks := setup(t)

							k := []byte("<key>")
							v := []byte("<value>")

							if err := ks.Set(t.Context(), k, v, 0); err != nil {
								t.Fatal(err)
							}

							err := ks.Set(t.Context(), k, v, c.Revision)

							expect := ConflictError[[]byte]{
								Keyspace: ks.Name(),
								Key:      k,
								Revision: c.Revision,
							}
							if !reflect.DeepEqual(err, expect) {
								t.Fatalf("unexpected error: got %q, want %q", err, expect)
							}

							if !IsConflict(err) {
								t.Fatalf("expected IsConflict to return true")
							}
						})
					}
				})
			})

			t.Run("when the key is not present in the keyspace", func(t *testing.T) {
				t.Run("it returns a ConflictError if the given revision is not zero", func(t *testing.T) {
					t.Parallel()

					ks := setup(t)

					k := []byte("<key>")
					v := []byte("<value>")

					err := ks.Set(t.Context(), k, v, 123)

					expect := ConflictError[[]byte]{
						Keyspace: ks.Name(),
						Key:      k,
						Revision: 123,
					}
					if !reflect.DeepEqual(err, expect) {
						t.Fatalf("unexpected error: got %q, want %q", err, expect)
					}

					if !IsConflict(err) {
						t.Fatalf("expected IsConflict to return true")
					}
				})

				t.Run("it allows deletion with a zero revision", func(t *testing.T) {
					t.Parallel()

					ks := setup(t)

					k := []byte("<key>")

					if err := ks.Set(t.Context(), k, nil, 0); err != nil {
						t.Fatal(err)
					}
				})
			})

			t.Run("it does not keep a reference to the key slice", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")
				v := []byte("<value>")

				if err := ks.Set(t.Context(), k, v, 0); err != nil {
					t.Fatal(err)
				}

				k[0] = 'X'

				ok, err := ks.Has(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}

				if ok {
					t.Fatalf("unexpected key: %q", string(k))
				}

				actual, _, err := ks.Get(t.Context(), []byte("<key>"))
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

				ks := setup(t)

				k := []byte("<key>")
				v := []byte("<value>")

				if err := ks.Set(t.Context(), k, v, 0); err != nil {
					t.Fatal(err)
				}

				v[0] = 'X'

				actual, _, err := ks.Get(t.Context(), k)
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

		t.Run("SetUnconditional", func(t *testing.T) {
			t.Parallel()

			t.Run("it always sets the value regardless of the current revision", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")

				if err := ks.SetUnconditional(t.Context(), k, []byte("<value-1>")); err != nil {
					t.Fatal(err)
				}

				actualValue, actualRevision, err := ks.Get(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}

				var (
					expectValue    = []byte("<value-1>")
					expectRevision = Revision(1)
				)

				if !bytes.Equal(expectValue, actualValue) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expectValue),
						string(actualValue),
					)
				}

				if expectRevision != actualRevision {
					t.Fatalf(
						"unexpected revision, want %d, got %d",
						expectRevision,
						actualRevision,
					)
				}

				if err := ks.SetUnconditional(t.Context(), k, []byte("<value-2>")); err != nil {
					t.Fatal(err)
				}

				actualValue, actualRevision, err = ks.Get(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}

				expectValue = []byte("<value-2>")
				expectRevision = 2

				if !bytes.Equal(expectValue, actualValue) {
					t.Fatalf(
						"unexpected value, want %q, got %q",
						string(expectValue),
						string(actualValue),
					)
				}

				if expectRevision != actualRevision {
					t.Fatalf(
						"unexpected revision, want %d, got %d",
						expectRevision,
						actualRevision,
					)
				}

				if err := ks.SetUnconditional(t.Context(), k, nil); err != nil {
					t.Fatal(err)
				}

				exists, err := ks.Has(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}

				if exists {
					t.Fatal("expected key to be deleted")
				}
			})
		})

		t.Run("Has", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns false if the key doesn't exist", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				ok, err := ks.Has(t.Context(), []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})

			t.Run("it returns true if the key exists", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")

				if err := ks.Set(t.Context(), k, []byte("<value>"), 0); err != nil {
					t.Fatal(err)
				}

				ok, err := ks.Has(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})

			t.Run("it returns false if the key has been deleted", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")

				if err := ks.Set(t.Context(), k, []byte("<value>"), 0); err != nil {
					t.Fatal(err)
				}

				if err := ks.Set(t.Context(), k, nil, 1); err != nil {
					t.Fatal(err)
				}

				ok, err := ks.Has(t.Context(), k)
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

				ks := setup(t)

				expect := map[string]struct {
					Value    string
					Revision Revision
				}{}

				for n := uint64(0); n < 100; n++ {
					k := fmt.Sprintf("<key-%d>", n)
					v := fmt.Sprintf("<value-%d>", n)

					if err := ks.Set(t.Context(), []byte(k), []byte(v), 0); err != nil {
						t.Fatal(err)
					}

					expect[k] = struct {
						Value    string
						Revision Revision
					}{v, 1}
				}

				actual := map[string]struct {
					Value    string
					Revision Revision
				}{}

				if err := ks.Range(
					t.Context(),
					func(_ context.Context, k, v []byte, r Revision) (bool, error) {
						actual[string(k)] = struct {
							Value    string
							Revision Revision
						}{string(v), r}

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

				ks := setup(t)

				for n := uint64(0); n < 2; n++ {
					k := fmt.Sprintf("<key-%d>", n)
					v := fmt.Sprintf("<value-%d>", n)

					if err := ks.Set(t.Context(), []byte(k), []byte(v), 0); err != nil {
						t.Fatal(err)
					}
				}

				called := false
				if err := ks.Range(
					t.Context(),
					func(_ context.Context, _, _ []byte, _ Revision) (bool, error) {
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

				ks := setup(t)

				expectKey := []byte("<key>")
				expectValue := []byte("<value>")

				if err := ks.Set(t.Context(), expectKey, expectValue, 0); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					t.Context(),
					func(_ context.Context, k, v []byte, _ Revision) (bool, error) {
						k[0] = 'X'
						v[0] = 'Y'

						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				mutatedKey := []byte("Xkey>")
				ok, err := ks.Has(t.Context(), mutatedKey)
				if err != nil {
					t.Fatal(err)
				}

				if ok {
					t.Fatalf("unexpected key: %q", string(mutatedKey))
				}

				actualValue, _, err := ks.Get(t.Context(), expectKey)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(expectValue, actualValue) {
					t.Fatalf(
						"unexpected value: got %q, want %q",
						string(actualValue),
						string(expectValue),
					)
				}
			})

			t.Run("it allows calls to Get() during iteration", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				if err := ks.Set(
					t.Context(),
					[]byte("<key>"),
					[]byte("<value>"),
					0,
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					t.Context(),
					func(ctx context.Context, k, expectValue []byte, expectRevision Revision) (bool, error) {
						actualValue, actualRevision, err := ks.Get(ctx, k)
						if err != nil {
							t.Fatal(err)
						}

						if !bytes.Equal(actualValue, expectValue) {
							t.Fatalf(
								"unexpected value: got %q, want %q",
								string(actualValue),
								string(expectValue),
							)
						}

						if actualRevision != expectRevision {
							t.Fatalf(
								"unexpected revision: got %d, want %d",
								actualRevision,
								expectRevision,
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

				ks := setup(t)

				if err := ks.Set(
					t.Context(),
					[]byte("<key>"),
					[]byte("<value>"),
					0,
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					t.Context(),
					func(ctx context.Context, k, _ []byte, _ Revision) (bool, error) {
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

				ks := setup(t)

				k := []byte("<key>")

				if err := ks.Set(
					t.Context(),
					k,
					[]byte("<value>"),
					0,
				); err != nil {
					t.Fatal(err)
				}

				expect := []byte("<updated>")

				if err := ks.Range(
					t.Context(),
					func(ctx context.Context, k, _ []byte, r Revision) (bool, error) {
						if err := ks.Set(ctx, k, expect, r); err != nil {
							t.Fatal(err)
						}
						return false, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				actual, _, err := ks.Get(t.Context(), k)
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
			ks, err := store.Open(t.Context(), xtesting.SequentialName("keyspace"))
			if err != nil {
				t.Fatal(err)
			}
			defer ks.Close()

			nonEmptyValue := rapid.StringN(1, -1, -1)

			pairs := map[string]struct {
				Value    []byte
				Revision Revision
			}{}

			var keys [][]byte

			t.Repeat(
				map[string]func(*rapid.T){
					"Get": func(t *rapid.T) {
						key := []byte(nonEmptyValue.Draw(t, "key"))

						actualValue, actualRevision, err := ks.Get(t.Context(), key)
						if err != nil {
							t.Fatal(err)
						}

						expect := pairs[string(key)]

						if !bytes.Equal(actualValue, expect.Value) {
							t.Fatalf(
								"unexpected value for key %q: got %q, want %q",
								string(key),
								string(actualValue),
								string(expect.Value),
							)
						}

						if actualRevision != expect.Revision {
							t.Fatalf(
								"unexpected revision for key %q: got %d, want %d",
								string(key),
								actualRevision,
								expect.Revision,
							)
						}
					},
					"Get (key exists)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")

						actualValue, actualRevision, err := ks.Get(t.Context(), key)
						if err != nil {
							t.Fatal(err)
						}

						expect := pairs[string(key)]

						if !bytes.Equal(actualValue, expect.Value) {
							t.Fatalf(
								"unexpected value for key %q: got %q, want %q",
								string(key),
								string(actualValue),
								string(expect.Value),
							)
						}

						if actualRevision != expect.Revision {
							t.Fatalf(
								"unexpected revision for key %q: got %d, want %d",
								string(key),
								actualRevision,
								expect.Revision,
							)
						}
					},
					"Has": func(t *rapid.T) {
						key := []byte(nonEmptyValue.Draw(t, "key"))

						ok, err := ks.Has(t.Context(), key)
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

						ok, err := ks.Has(t.Context(), key)
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
					"Set (new key)": func(t *rapid.T) {
						key := []byte(
							nonEmptyValue.
								Filter(func(s string) bool {
									_, exists := pairs[s]
									return !exists
								}).
								Draw(t, "key"),
						)
						value := []byte(nonEmptyValue.Draw(t, "value"))

						if err := ks.Set(t.Context(), key, value, 0); err != nil {
							t.Fatal(err)
						}

						pairs[string(key)] = struct {
							Value    []byte
							Revision Revision
						}{value, 1}

						keys = append(keys, key)
					},
					"Set (existing key, new value)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")
						item := pairs[string(key)]

						value := []byte(
							nonEmptyValue.
								Filter(func(s string) bool {
									return !bytes.Equal([]byte(s), item.Value)
								}).
								Draw(t, "value"),
						)

						if err := ks.Set(t.Context(), key, value, item.Revision); err != nil {
							t.Fatal(err)
						}

						item.Value = value
						item.Revision++

						pairs[string(key)] = item
					},
					"Set (existing key, same value)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")
						item := pairs[string(key)]

						if err := ks.Set(t.Context(), key, item.Value, item.Revision); err != nil {
							t.Fatal(err)
						}

						item.Revision++
						pairs[string(key)] = item
					},
					"Set (delete)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")
						item := pairs[string(key)]

						if err := ks.Set(t.Context(), key, nil, item.Revision); err != nil {
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
							t.Context(),
							func(_ context.Context, k, actualValue []byte, actualRevision Revision) (bool, error) {
								if _, ok := seen[string(k)]; ok {
									t.Fatalf(
										"key seen twice while ranging over pairs: %q",
										string(k),
									)
								}
								seen[string(k)] = struct{}{}

								expect := pairs[string(k)]

								if !bytes.Equal(actualValue, expect.Value) {
									t.Fatalf(
										"unexpected value for key %q: got %q, want %q",
										string(k),
										string(actualValue),
										string(expect.Value),
									)
								}

								if actualRevision != expect.Revision {
									t.Fatalf(
										"unexpected revision for key %q: got %d, want %d",
										string(k),
										actualRevision,
										expect.Revision,
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
