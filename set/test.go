package set

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/dogmatiq/persistencekit/internal/testx"
	"pgregory.net/rapid"
)

// RunTests runs tests that confirm a [BinaryStore] implementation behaves correctly.
func RunTests(
	t *testing.T,
	store BinaryStore,
) {
	setup := func(t *testing.T) BinarySet {
		name := testx.SequentialName("set")

		set, err := store.Open(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := set.Close(); err != nil {
				t.Error(err)
			}
		})

		if set.Name() != name {
			t.Fatalf("unexpected set name: got %q, want %q", set.Name(), name)
		}

		return set
	}

	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows sets to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				s1, err := store.Open(t.Context(), "<set>")
				if err != nil {
					t.Fatal(err)
				}
				defer s1.Close()

				s2, err := store.Open(t.Context(), "<set>")
				if err != nil {
					t.Fatal(err)
				}
				defer s2.Close()

				if err := s1.Add(t.Context(), []byte("<value>")); err != nil {
					t.Fatal(err)
				}

				ok, err := s2.Has(t.Context(), []byte("<value>"))
				if err != nil {
					t.Fatal(err)
				}

				if !ok {
					t.Fatal("expected value to be present")
				}
			})
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Parallel()

		t.Run("Has", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns false if the value is not present", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				ok, err := set.Has(t.Context(), []byte("<value>"))
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})

			t.Run("it returns true if the value is present", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v := []byte("<value>")

				if err := set.Add(t.Context(), v); err != nil {
					t.Fatal(err)
				}

				ok, err := set.Has(t.Context(), v)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})

			t.Run("it returns false if the value has been removed", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v := []byte("<value>")

				if err := set.Add(t.Context(), v); err != nil {
					t.Fatal(err)
				}

				if err := set.Remove(t.Context(), v); err != nil {
					t.Fatal(err)
				}

				ok, err := set.Has(t.Context(), v)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})

			t.Run("it returns false if the value is not present, but others are", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v1 := []byte("<value-1>")
				v2 := []byte("<value-2>")

				if err := set.Add(t.Context(), v1); err != nil {
					t.Fatal(err)
				}

				ok, err := set.Has(t.Context(), v2)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})
		})

		t.Run("Add", func(t *testing.T) {
			t.Parallel()

			t.Run("it does not keep a reference to the value slice", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v := []byte("<value>")

				if err := set.Add(t.Context(), v); err != nil {
					t.Fatal(err)
				}

				v[0] = 'X'

				ok, err := set.Has(t.Context(), []byte("<value>"))
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}

				ok, err = set.Has(t.Context(), v)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})
		})

		t.Run("TryAdd", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns true if the value was added", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				ok, err := set.TryAdd(t.Context(), []byte("<value>"))
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})

			t.Run("it returns false if the value was already present", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v := []byte("<value>")

				if err := set.Add(t.Context(), v); err != nil {
					t.Fatal(err)
				}

				ok, err := set.TryAdd(t.Context(), v)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})

			t.Run("it does not affect other values", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v1 := []byte("<value-1>")
				v2 := []byte("<value-2>")

				if err := set.Add(t.Context(), v1); err != nil {
					t.Fatal(err)
				}
				if err := set.Add(t.Context(), v2); err != nil {
					t.Fatal(err)
				}

				if err := set.Remove(t.Context(), v1); err != nil {
					t.Fatal(err)
				}

				ok, err := set.Has(t.Context(), v2)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})
		})

		t.Run("TryRemove", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns true if the value was removed", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v := []byte("<value>")

				if err := set.Add(t.Context(), v); err != nil {
					t.Fatal(err)
				}

				ok, err := set.TryRemove(t.Context(), v)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})

			t.Run("it returns false if the value was not present", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				ok, err := set.TryRemove(t.Context(), []byte("<value>"))
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					t.Fatal("expected ok to be false")
				}
			})

			t.Run("it does not affect other values", func(t *testing.T) {
				t.Parallel()

				set := setup(t)

				v1 := []byte("<value-1>")
				v2 := []byte("<value-2>")

				if err := set.Add(t.Context(), v1); err != nil {
					t.Fatal(err)
				}
				if err := set.Add(t.Context(), v2); err != nil {
					t.Fatal(err)
				}

				if _, err := set.TryRemove(t.Context(), v1); err != nil {
					t.Fatal(err)
				}

				ok, err := set.Has(t.Context(), v2)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected ok to be true")
				}
			})
		})

		t.Run("property-based", func(t *testing.T) {
			t.Parallel()

			rapid.Check(t, func(t *rapid.T) {
				set, err := store.Open(t.Context(), testx.SequentialName("set"))
				if err != nil {
					t.Fatal(err)
				}
				defer set.Close()

				nonEmptyValue := rapid.StringN(1, -1, -1)

				membership := map[string]struct{}{}
				var values [][]byte

				t.Repeat(
					map[string]func(*rapid.T){
						"Has": func(t *rapid.T) {
							value := []byte(nonEmptyValue.Draw(t, "value"))

							ok, err := set.Has(t.Context(), value)
							if err != nil {
								t.Fatal(err)
							}

							_, expect := membership[string(value)]
							if ok != expect {
								t.Fatalf(
									"unexpected has for key %q: got %t, want %t",
									string(value),
									ok,
									expect,
								)
							}
						},
						"Has (value is present)": func(t *rapid.T) {
							if len(membership) == 0 {
								t.Skip("skip: set is empty")
							}

							value := rapid.SampledFrom(values).Draw(t, "value")

							ok, err := set.Has(t.Context(), value)
							if err != nil {
								t.Fatal(err)
							}

							expect := true
							if ok != expect {
								t.Fatalf(
									"unexpected has for value %q: got %t, want %t",
									string(value),
									ok,
									expect,
								)
							}
						},
						"Add": func(t *rapid.T) {
							value := []byte(nonEmptyValue.Draw(t, "value"))

							if err := set.Add(t.Context(), value); err != nil {
								t.Fatal(err)
							}

							n := len(membership)
							membership[string(value)] = struct{}{}
							if len(membership) > n {
								values = append(values, value)
							}
						},
						"TryAdd": func(t *rapid.T) {
							value := []byte(nonEmptyValue.Draw(t, "value"))

							ok, err := set.TryAdd(t.Context(), value)
							if err != nil {
								t.Fatal(err)
							}

							if ok {
								membership[string(value)] = struct{}{}
								values = append(values, value)
							}
						},
						"Remove": func(t *rapid.T) {
							if len(membership) == 0 {
								t.Skip("skip: set is empty")
							}

							value := rapid.SampledFrom(values).Draw(t, "value")

							if err := set.Remove(t.Context(), value); err != nil {
								t.Fatal(err)
							}

							delete(membership, string(value))
							values = slices.DeleteFunc(
								values,
								func(k []byte) bool {
									return bytes.Equal(k, value)
								},
							)
						},
						"TryRemove": func(t *rapid.T) {
							value := []byte(nonEmptyValue.Draw(t, "value"))

							ok, err := set.TryRemove(t.Context(), value)
							if err != nil {
								t.Fatal(err)
							}

							if ok {
								delete(membership, string(value))
								values = slices.DeleteFunc(
									values,
									func(k []byte) bool {
										return bytes.Equal(k, value)
									},
								)
							}
						},
						"TryRemove (value is present)": func(t *rapid.T) {
							if len(membership) == 0 {
								t.Skip("skip: set is empty")
							}

							value := rapid.SampledFrom(values).Draw(t, "value")

							ok, err := set.TryRemove(t.Context(), value)
							if err != nil {
								t.Fatal(err)
							}

							if !ok {
								t.Fatalf("expected value %q to be removed", string(value))
							}

							delete(membership, string(value))
							values = slices.DeleteFunc(
								values,
								func(k []byte) bool {
									return bytes.Equal(k, value)
								},
							)
						},
						"Range": func(t *rapid.T) {
							visited := map[string]struct{}{}

							err := set.Range(
								t.Context(),
								func(
									_ context.Context,
									v []byte,
								) (bool, error) {
									visited[string(v)] = struct{}{}
									return true, nil
								},
							)
							if err != nil {
								t.Fatal(err)
							}

							if len(visited) != len(membership) {
								t.Fatalf(
									"unexpected number of values: got %d, want %d",
									len(visited),
									len(membership),
								)
							}

							for v := range membership {
								if _, ok := visited[v]; !ok {
									t.Fatalf("missing value from range: %q", v)
								}
							}
						},
					},
				)
			})
		})
	})
}
