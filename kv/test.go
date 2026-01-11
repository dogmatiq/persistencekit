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
				if _, err := ks1.Set(t.Context(), []byte("<key>"), expect, nil); err != nil {
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

				v, ct, err := ks.Get(t.Context(), []byte("<key>"))
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 0 {
					t.Fatal("expected zero-length value")
				}
				if len(ct) != 0 {
					t.Fatal("expected zero-length token")
				}
			})

			t.Run("it returns an empty value if the key has been deleted", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")

				ct, err := ks.Set(t.Context(), k, []byte("<value>"), nil)
				if err != nil {
					t.Fatal(err)
				}

				if _, err = ks.Set(t.Context(), k, nil, ct); err != nil {
					t.Fatal(err)
				}

				v, ct, err := ks.Get(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 0 {
					t.Fatal("expected zero-length value")
				}
				if len(ct) != 0 {
					t.Fatal("expected zero-length token")
				}
			})

			t.Run("it returns the value if the key exists", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				for i := 0; i < 5; i++ {
					k := []byte(fmt.Sprintf("<key-%d>", i))
					v := []byte(fmt.Sprintf("<value-%d>", i))

					if _, err := ks.Set(t.Context(), k, v, nil); err != nil {
						t.Fatal(err)
					}
				}

				for i := 0; i < 5; i++ {
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

				if _, err := ks.Set(t.Context(), k, []byte("<value>"), nil); err != nil {
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
				t.Run("it returns a ConflictError if the concurrency token is empty", func(t *testing.T) {
					t.Parallel()

					ks := setup(t)

					k := []byte("<key>")
					v := []byte("<value>")

					if _, err := ks.Set(t.Context(), k, v, nil); err != nil {
						t.Fatal(err)
					}

					_, err := ks.Set(t.Context(), k, v, nil)

					expect := ConflictError[[]byte]{
						Keyspace: ks.Name(),
						Key:      k,
						Token:    nil,
					}
					if !reflect.DeepEqual(err, expect) {
						t.Fatalf("unexpected error: got %q, want %q", err, expect)
					}
				})

				t.Run("it returns a ConflictError if the concurrency token does not match", func(t *testing.T) {
					t.Parallel()

					ks := setup(t)

					k := []byte("<key>")
					v := []byte("<value>")

					ct, err := ks.Set(t.Context(), k, v, nil)
					ct = append(ct, 0) // modify token to make it invalid

					_, err = ks.Set(t.Context(), k, v, ct)

					expect := ConflictError[[]byte]{
						Keyspace: ks.Name(),
						Key:      k,
						Token:    ct,
					}
					if !reflect.DeepEqual(err, expect) {
						t.Fatalf("unexpected error: got %q, want %q", err, expect)
					}
				})
			})

			t.Run("when the key is not present in the keyspace", func(t *testing.T) {
				t.Run("it returns a ConflictError if the concurrency token is not empty", func(t *testing.T) {
					t.Parallel()

					ks := setup(t)

					k := []byte("<key>")
					v := []byte("<value>")

					_, err := ks.Set(t.Context(), k, v, []byte("<invalid-token>"))

					expect := ConflictError[[]byte]{
						Keyspace: ks.Name(),
						Key:      k,
						Token:    []byte("<invalid-token>"),
					}
					if !reflect.DeepEqual(err, expect) {
						t.Fatalf("unexpected error: got %q, want %q", err, expect)
					}
				})

				t.Run("it succeeds if deleting with an empty concurrency token", func(t *testing.T) {
					t.Parallel()

					ks := setup(t)

					k := []byte("<key>")

					ct, err := ks.Set(t.Context(), k, nil, nil)
					if err != nil {
						t.Fatal(err)
					}

					if len(ct) != 0 {
						t.Fatal("expected zero-length token after deleting key")
					}
				})
			})

			t.Run("it does not keep a reference to the key slice", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				k := []byte("<key>")
				v := []byte("<value>")

				if _, err := ks.Set(t.Context(), k, v, nil); err != nil {
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

				if _, err := ks.Set(t.Context(), k, v, nil); err != nil {
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

				if _, err := ks.Set(t.Context(), k, []byte("<value>"), nil); err != nil {
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

				ct, err := ks.Set(t.Context(), k, []byte("<value>"), nil)
				if err != nil {
					t.Fatal(err)
				}

				if _, err := ks.Set(t.Context(), k, nil, ct); err != nil {
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
					Value string
					Token []byte
				}{}

				for n := uint64(0); n < 100; n++ {
					k := fmt.Sprintf("<key-%d>", n)
					v := fmt.Sprintf("<value-%d>", n)
					ct, err := ks.Set(t.Context(), []byte(k), []byte(v), nil)
					if err != nil {
						t.Fatal(err)
					}

					expect[k] = struct {
						Value string
						Token []byte
					}{v, ct}
				}

				actual := map[string]struct {
					Value string
					Token []byte
				}{}

				if err := ks.Range(
					t.Context(),
					func(_ context.Context, k, v, t []byte) (bool, error) {
						actual[string(k)] = struct {
							Value string
							Token []byte
						}{string(v), t}

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
					if _, err := ks.Set(t.Context(), []byte(k), []byte(v), nil); err != nil {
						t.Fatal(err)
					}
				}

				called := false
				if err := ks.Range(
					t.Context(),
					func(_ context.Context, _, _, _ []byte) (bool, error) {
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

				expectToken, err := ks.Set(t.Context(), expectKey, expectValue, nil)
				if err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					t.Context(),
					func(_ context.Context, k, v, t []byte) (bool, error) {
						k[0] = 'X'
						v[0] = 'Y'
						t[0] = 'Z'

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

				actualValue, actualToken, err := ks.Get(t.Context(), expectKey)
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

				if !bytes.Equal(expectToken, actualToken) {
					t.Fatalf(
						"unexpected token: got %x, want %x",
						actualToken,
						expectToken,
					)
				}
			})

			t.Run("it allows calls to Get() during iteration", func(t *testing.T) {
				t.Parallel()

				ks := setup(t)

				if _, err := ks.Set(
					t.Context(),
					[]byte("<key>"),
					[]byte("<value>"),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					t.Context(),
					func(ctx context.Context, k, expectValue, expectToken []byte) (bool, error) {
						actualValue, actualToken, err := ks.Get(ctx, k)
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

						if !bytes.Equal(expectToken, actualToken) {
							t.Fatalf(
								"unexpected token: got %x, want %x",
								actualToken,
								expectToken,
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

				if _, err := ks.Set(
					t.Context(),
					[]byte("<key>"),
					[]byte("<value>"),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				if err := ks.Range(
					t.Context(),
					func(ctx context.Context, k, _, _ []byte) (bool, error) {
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

				if _, err := ks.Set(
					t.Context(),
					k,
					[]byte("<value>"),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				expect := []byte("<updated>")

				if err := ks.Range(
					t.Context(),
					func(ctx context.Context, k, _, ct []byte) (bool, error) {
						if _, err := ks.Set(ctx, k, expect, ct); err != nil {
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

			pairs := map[string][]byte{}
			tokens := map[string][]byte{}
			var keys [][]byte

			t.Repeat(
				map[string]func(*rapid.T){
					"Get": func(t *rapid.T) {
						key := []byte(nonEmptyValue.Draw(t, "key"))

						value, token, err := ks.Get(t.Context(), key)
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

						expect = tokens[string(key)]
						if !bytes.Equal(expect, token) {
							t.Fatalf(
								"unexpected token for key %q: got %x, want %x",
								string(key),
								token,
								expect,
							)
						}
					},
					"Get (key exists)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")

						value, token, err := ks.Get(t.Context(), key)
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

						expect = tokens[string(key)]
						if !bytes.Equal(expect, token) {
							t.Fatalf(
								"unexpected token for key %q: got %x, want %x",
								string(key),
								token,
								expect,
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

						token, err := ks.Set(t.Context(), key, value, nil)
						if err != nil {
							t.Fatal(err)
						}

						pairs[string(key)] = value
						tokens[string(key)] = token
						keys = append(keys, key)
					},
					"Set (existing key)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")

						oldValue := pairs[string(key)]
						newValue := []byte(
							nonEmptyValue.
								Filter(func(s string) bool {
									return !bytes.Equal([]byte(s), oldValue)
								}).
								Draw(t, "value"),
						)

						oldToken := tokens[string(key)]
						newToken, err := ks.Set(t.Context(), key, newValue, oldToken)
						if err != nil {
							t.Fatal(err)
						}

						if bytes.Equal(oldToken, newToken) {
							t.Fatal("concurrency token must change when value is changed")
						}

						pairs[string(key)] = newValue
						tokens[string(key)] = newToken
					},
					"Set (delete)": func(t *rapid.T) {
						if len(pairs) == 0 {
							t.Skip("skip: keyspace is empty")
						}

						key := rapid.SampledFrom(keys).Draw(t, "key")
						token := tokens[string(key)]

						token, err := ks.Set(t.Context(), key, nil, token)
						if err != nil {
							t.Fatal(err)
						}

						if len(token) != 0 {
							t.Fatal("expected zero-length token after deleting key")
						}

						delete(pairs, string(key))
						delete(tokens, string(key))

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
							func(_ context.Context, k, v, ct []byte) (bool, error) {
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

								expect = tokens[string(k)]
								if !bytes.Equal(expect, ct) {
									t.Fatalf(
										"unexpected token for key %q: got %x, want %x",
										string(k),
										ct,
										expect,
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
