package journal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/dogmatiq/persistencekit/internal/testx"
	"github.com/google/go-cmp/cmp"
	"pgregory.net/rapid"
)

// RunTests runs tests that confirm a journal implementation behaves correctly.
func RunTests(
	t *testing.T,
	store BinaryStore,
) {
	setup := func(t *testing.T) BinaryJournal {
		name := testx.SequentialName("journal")

		j, err := store.Open(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := j.Close(); err != nil {
				t.Error(err)
			}
		})

		if j.Name() != name {
			t.Fatalf("unexpected journal name: got %q, want %q", j.Name(), name)
		}

		return j
	}

	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows a journal to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				name := testx.SequentialName("journal")

				j1, err := store.Open(t.Context(), name)
				if err != nil {
					t.Fatal(err)
				}
				defer j1.Close()

				j2, err := store.Open(t.Context(), name)
				if err != nil {
					t.Fatal(err)
				}
				defer j2.Close()

				want := []byte("<record>")
				if err := j1.Append(t.Context(), 0, want); err != nil {
					t.Fatal(err)
				}

				got, err := j2.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(got, want) {
					t.Fatalf("unexpected record: got %q, want %q", string(got), string(want))
				}
			})
		})
	})

	t.Run("Journal", func(t *testing.T) {
		t.Parallel()

		t.Run("Bounds", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns the expected bounds", func(t *testing.T) {
				cases := []struct {
					Name   string
					Expect Interval
					Setup  func(*testing.T, BinaryJournal)
				}{
					{
						"empty",
						Interval{0, 0},
						func(*testing.T, BinaryJournal) {},
					},
					{
						"with records",
						Interval{0, 10},
						func(t *testing.T, j BinaryJournal) {
							appendRecords(t, j, 10)
						},
					},
					{
						"with some records truncated",
						Interval{5, 10},
						func(t *testing.T, j BinaryJournal) {
							appendRecords(t, j, 10)
							if err := j.Truncate(t.Context(), 5); err != nil {
								t.Fatal(err)
							}
						},
					},
					{
						"with all records truncated",
						Interval{10, 10},
						func(t *testing.T, j BinaryJournal) {
							appendRecords(t, j, 10)
							if err := j.Truncate(t.Context(), 10); err != nil {
								t.Fatal(err)
							}
						},
					},
				}

				for _, c := range cases {
					t.Run(c.Name, func(t *testing.T) {
						t.Parallel()

						j := setup(t)

						c.Setup(t, j)

						bounds, err := j.Bounds(t.Context())
						if err != nil {
							t.Fatal(err)
						}

						if bounds != c.Expect {
							t.Fatalf(
								"unexpected bounds: got %s, want %s",
								bounds,
								c.Expect,
							)
						}
					})
				}
			})
		})

		t.Run("Get", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns a RecordNotFoundError if there is no record at the given position", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				_, err := j.Get(t.Context(), 1)

				expect := RecordNotFoundError{
					Journal:  j.Name(),
					Position: 1,
				}
				if !errors.Is(err, expect) {
					t.Fatalf("unexpected error: got %q, want %q", err, expect)
				}
			})

			t.Run("it returns the record if it exists", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				// Ensure we test with a position that becomes 2 digits long to
				// confirm that the implementation is not using a lexical sort.
				records := appendRecords(t, j, 15)

				for i, want := range records {
					got, err := j.Get(t.Context(), Position(i))
					if err != nil {
						t.Fatal(err)
					}

					if !bytes.Equal(want, got) {
						t.Fatalf(
							"unexpected record at position %d, want %q, got %q",
							i,
							string(want),
							string(got),
						)
					}
				}
			})

			t.Run("it returns a RecordNotFoundError if the record has been truncated", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				const recordCount = 5
				const truncateBefore = 3
				records := appendRecords(t, j, recordCount)

				err := j.Truncate(t.Context(), truncateBefore)
				if err != nil {
					t.Fatal(err)
				}

				for pos, want := range records {
					pos := Position(pos)

					if pos < truncateBefore {
						expect := RecordNotFoundError{
							Journal:  j.Name(),
							Position: pos,
						}
						if _, err := j.Get(t.Context(), pos); err != expect {
							t.Fatalf("unexpected error at position %d: got %q, want %q", pos, err, expect)
						}
					} else {
						got, err := j.Get(t.Context(), pos)
						if err != nil {
							t.Fatal(err)
						}

						if !bytes.Equal(want, got) {
							t.Fatalf(
								"unexpected record at position %d, want %q, got %q",
								pos,
								string(want),
								string(got),
							)
						}
					}
				}
			})

			t.Run("it does not return any records when all records are truncated", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				records := appendRecords(t, j, 5)

				err := j.Truncate(t.Context(), 5)
				if err != nil {
					t.Fatal(err)
				}

				for i := range records {
					pos := Position(i)
					expect := RecordNotFoundError{
						Journal:  j.Name(),
						Position: pos,
					}
					if _, err := j.Get(t.Context(), pos); err != expect {
						t.Fatalf("unexpected error at position %d: got %q, want %q", pos, err, expect)
					}
				}
			})

			t.Run("it does not return its internal byte slice", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 1)

				rec, err := j.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				rec[0] = 'X'

				got, err := j.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				want := []byte("<record-0>")
				if !bytes.Equal(got, want) {
					t.Fatalf(
						"unexpected record: got %q, want %q",
						string(got),
						string(want),
					)
				}
			})

			t.Run("handles maximum position value", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				_, err := j.Get(t.Context(), math.MaxUint64)
				if !IsNotFound(err) {
					t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
				}
			})
		})

		t.Run("Range", func(t *testing.T) {
			t.Parallel()

			t.Run("calls the function for each record in the journal", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				want := appendRecords(t, j, 15)

				var got [][]byte
				wantPos := Position(10)
				want = want[wantPos:]

				if err := j.Range(
					t.Context(),
					wantPos,
					func(_ context.Context, gotPos Position, rec []byte) (bool, error) {
						if gotPos != wantPos {
							t.Fatalf("unexpected position: got %d, want %d", gotPos, wantPos)
						}

						got = append(got, rec)
						wantPos++

						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatal(diff)
				}
			})

			t.Run("it stops iterating if the function returns false", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 2)

				called := false
				if err := j.Range(
					t.Context(),
					0,
					func(context.Context, Position, []byte) (bool, error) {
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

			t.Run("it returns a RecordNotFoundError if the journal is empty", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				err := j.Range(
					t.Context(),
					0,
					func(context.Context, Position, []byte) (bool, error) {
						t.Fatal("unexpected call")
						return false, nil
					},
				)

				expect := RecordNotFoundError{
					Journal:  j.Name(),
					Position: 0,
				}
				if err != expect {
					t.Fatalf("unexpected error: got %q, want %q", err, expect)
				}
			})

			t.Run("it does not range over truncated records", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				const recordCount = 5
				const truncateBefore = 3
				records := appendRecords(t, j, recordCount)

				err := j.Truncate(t.Context(), truncateBefore)
				if err != nil {
					t.Fatal(err)
				}

				for pos, want := range records {
					pos := Position(pos)

					if pos < truncateBefore {
						expect := RecordNotFoundError{
							Journal:  j.Name(),
							Position: pos,
						}

						if err := j.Range(
							t.Context(),
							pos,
							func(context.Context, Position, []byte) (bool, error) {
								t.Fatal("unexpected call")
								return false, nil
							},
						); err != expect {
							t.Fatalf("unexpected error: got %q, want %q", err, expect)
						}
					} else {
						if err := j.Range(
							t.Context(),
							pos,
							func(_ context.Context, pos Position, got []byte) (bool, error) {
								if !bytes.Equal(want, got) {
									return false, fmt.Errorf(
										"unexpected record at position %d, want %q, got %q",
										pos,
										string(want),
										string(got),
									)
								}
								return false, nil
							},
						); err != nil {
							t.Fatal(err)
						}
					}
				}
			})

			t.Run("it does not range over truncated records when all records are truncated", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				records := appendRecords(t, j, 5)

				err := j.Truncate(t.Context(), 5)
				if err != nil {
					t.Fatal(err)
				}

				for pos := range records {
					pos := Position(pos)
					expect := RecordNotFoundError{
						Journal:  j.Name(),
						Position: pos,
					}

					if err := j.Range(
						t.Context(),
						pos,
						func(context.Context, Position, []byte) (bool, error) {
							t.Fatal("unexpected call")
							return false, nil
						},
					); err != expect {
						t.Fatalf("unexpected error: got %q, want %q", err, expect)
					}
				}
			})

			t.Run("it returns an error if a record is truncated during iteration", func(t *testing.T) {
				t.Skip("not implemented") // TODO
			})

			t.Run("it does not invoke the function with its internal byte slice", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 1)

				if err := j.Range(
					t.Context(),
					0,
					func(_ context.Context, _ Position, rec []byte) (bool, error) {
						rec[0] = 'X'
						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				got, err := j.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				want := []byte("<record-0>")
				if !bytes.Equal(got, want) {
					t.Fatalf(
						"unexpected record: got %q, want %q",
						string(got),
						string(want),
					)
				}
			})

			t.Run("handles maximum position value", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				err := j.Range(
					t.Context(),
					math.MaxUint64,
					func(context.Context, Position, []byte) (bool, error) {
						t.Fatal("unexpected call")
						return false, nil
					},
				)

				if !IsNotFound(err) {
					t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
				}
			})
		})

		t.Run("Append", func(t *testing.T) {
			t.Parallel()

			t.Run("it does not return an error if there is no record at the given position", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				if err := j.Append(t.Context(), 0, []byte("<record>")); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it returns a ConflictError if there is already a record at the given position", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				if err := j.Append(t.Context(), 0, []byte("<prior>")); err != nil {
					t.Fatal(err)
				}

				want := []byte("<original>")
				if err := j.Append(t.Context(), 1, want); err != nil {
					t.Fatal(err)
				}

				err := j.Append(t.Context(), 1, []byte("<conflicting>"))

				expect := ConflictError{
					Journal:  j.Name(),
					Position: 1,
				}
				if err != expect {
					t.Fatalf("unexpected error: got %q, want %q", err, expect)
				}

				got, err := j.Get(t.Context(), 1)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(got, want) {
					t.Fatalf(
						"unexpected record: got %q, want %q",
						string(got),
						string(want),
					)
				}
			})

			t.Run("it returns a ConflictError if there is a truncated record at the given position", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 2)
				if err := j.Truncate(t.Context(), 2); err != nil {
					t.Fatal(err)
				}

				err := j.Append(t.Context(), 0, []byte("<conflicting>"))

				expect := ConflictError{
					Journal:  j.Name(),
					Position: 0,
				}
				if err != expect {
					t.Fatalf("unexpected error: got %q, want %q", err, expect)
				}

				_, err = j.Get(t.Context(), 0)
				if !IsNotFound(err) {
					t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
				}

				expect = ConflictError{
					Journal:  j.Name(),
					Position: 1,
				}
				err = j.Append(t.Context(), 1, []byte("<conflicting>"))
				if err != expect {
					t.Fatalf("unexpected error: got %q, want %q", err, expect)
				}

				_, err = j.Get(t.Context(), 1)
				if !IsNotFound(err) {
					t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
				}
			})

			t.Run("it does not keep a reference to the record slice", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				rec := []byte("<record>")

				if err := j.Append(t.Context(), 0, rec); err != nil {
					t.Fatal(err)
				}

				rec[0] = 'X'

				got, err := j.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				want := []byte("<record>")
				if !bytes.Equal(got, want) {
					t.Fatalf("unexpected record: got %q, want %q", string(got), string(want))
				}
			})

			t.Run("it does not conflate records from separate journals", func(t *testing.T) {
				t.Parallel()

				j1 := setup(t)
				j2 := setup(t)

				want1 := []byte("<record-j1>")
				want2 := []byte("<record-j2>")

				if err := j1.Append(t.Context(), 0, want1); err != nil {
					t.Fatal(err)
				}

				if err := j2.Append(t.Context(), 0, want2); err != nil {
					t.Fatal(err)
				}

				got1, err := j1.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(got1, want1) {
					t.Fatalf("unexpected record in j1: got %q, want %q", string(got1), string(want1))
				}

				got2, err := j2.Get(t.Context(), 0)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(got2, want2) {
					t.Fatalf("unexpected record in j2: got %q, want %q", string(got2), string(want2))
				}
			})
		})

		t.Run("Truncate", func(t *testing.T) {
			t.Parallel()

			t.Run("it truncates the journal", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 3)

				if err := j.Truncate(t.Context(), 1); err != nil {
					t.Fatal(err)
				}

				got, err := j.Bounds(t.Context())
				if err != nil {
					t.Fatal(err)
				}

				want := Position(1)
				if got.Begin != want {
					t.Fatalf("unexpected begin position: got %s, want %d", got, want)
				}
			})

			t.Run("it allows truncating all records", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 3)

				if err := j.Truncate(t.Context(), 3); err != nil {
					t.Fatal(err)
				}

				got, err := j.Bounds(t.Context())
				if err != nil {
					t.Fatal(err)
				}

				want := Interval{3, 3}

				if got != want {
					t.Fatalf("unexpected bounds: got %s, want %s", got, want)
				}
			})

			t.Run("it does not fail when the records have already been truncated", func(t *testing.T) {
				t.Parallel()

				j := setup(t)

				appendRecords(t, j, 3)

				if err := j.Truncate(t.Context(), 1); err != nil {
					t.Fatal(err)
				}

				if err := j.Truncate(t.Context(), 2); err != nil {
					t.Fatal(err)
				}

				got, err := j.Bounds(t.Context())
				if err != nil {
					t.Fatal(err)
				}

				want := Position(2)
				if got.Begin != want {
					t.Fatalf("unexpected begin position: got %s, want %d", got, want)
				}
			})
		})
	})

	t.Run("property-based", func(t *testing.T) {
		t.Parallel()

		rapid.Check(t, func(t *rapid.T) {
			j, err := store.Open(t.Context(), testx.SequentialName("journal"))
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()

			var bounds Interval
			var records []string

			t.Repeat(
				map[string]func(*rapid.T){
					"": func(t *rapid.T) {
						got, err := j.Bounds(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						if got != bounds {
							t.Fatalf("unexpected bounds: got %s, want %s", got, bounds)
						}
					},
					"Get (success)": func(t *rapid.T) {
						if bounds.IsEmpty() {
							t.Skip("skip: journal is empty")
						}

						pos := Position(
							rapid.Uint64Range(
								uint64(bounds.Begin),
								uint64(bounds.End-1),
							).Draw(t, "pos"),
						)

						rec, err := j.Get(t.Context(), pos)
						if err != nil {
							t.Fatal(err)
						}

						expect := records[pos]
						if string(rec) != expect {
							t.Fatalf(
								"unexpected record at position %d: got %q, want %q",
								pos,
								string(rec),
								string(expect),
							)
						}
					},
					"Get (truncated)": func(t *rapid.T) {
						if bounds.Begin == 0 {
							t.Skip("skip: no records have been truncated")
						}

						pos := Position(
							rapid.Uint64Range(
								uint64(0),
								uint64(bounds.Begin-1),
							).Draw(t, "pos"),
						)

						_, err := j.Get(t.Context(), pos)
						if !IsNotFound(err) {
							t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
						}
					},
					"Get (future)": func(t *rapid.T) {
						pos := Position(
							rapid.Uint64Min(
								uint64(bounds.End),
							).Draw(t, "pos"),
						)

						_, err := j.Get(t.Context(), pos)
						if !IsNotFound(err) {
							t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
						}
					},
					"Range (all)": func(t *rapid.T) {
						if bounds.IsEmpty() {
							t.Skip("skip: journal is empty")
						}

						wantPos := bounds.Begin

						if err := j.Range(
							t.Context(),
							wantPos,
							func(_ context.Context, gotPos Position, gotRec []byte) (bool, error) {
								if gotPos != wantPos {
									return false, fmt.Errorf(
										"unexpected position: got %d, want %d",
										gotPos,
										wantPos,
									)
								}

								wantRec := records[gotPos]
								if string(gotRec) != wantRec {
									return false, fmt.Errorf(
										"unexpected record at position %d: got %q, want %q",
										gotPos,
										gotRec,
										string(wantRec),
									)
								}

								wantPos++
								return true, nil
							},
						); err != nil {
							t.Fatal(err)
						}
					},
					"Range (truncated)": func(t *rapid.T) {
						if bounds.Begin == 0 {
							t.Skip("skip: no records have been truncated")
						}

						pos := Position(
							rapid.Uint64Range(
								uint64(0),
								uint64(bounds.Begin-1),
							).Draw(t, "pos"),
						)

						if err := j.Range(
							t.Context(),
							pos,
							func(context.Context, Position, []byte) (bool, error) {
								return false, errors.New("unexpected call")
							},
						); !IsNotFound(err) {
							t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
						}
					},
					"Range (future)": func(t *rapid.T) {
						pos := Position(
							rapid.Uint64Min(
								uint64(bounds.End),
							).Draw(t, "pos"),
						)

						if err := j.Range(
							t.Context(),
							pos,
							func(context.Context, Position, []byte) (bool, error) {
								return false, errors.New("unexpected call")
							},
						); !IsNotFound(err) {
							t.Fatalf("unexpected error: got %q, want IsNotFound(err) == true", err)
						}
					},
					"Append (success)": func(t *rapid.T) {
						rec := rapid.String().Draw(t, "rec")

						err := j.Append(t.Context(), bounds.End, []byte(rec))
						if err != nil {
							t.Fatalf("unable to append record at position %d: %s", bounds.End, err)
						}

						records = append(records, rec)
						bounds.End++

						t.Logf("appended record at position %d, bounds are now %s", bounds.End-1, bounds)
					},
					"Append (conflict)": func(t *rapid.T) {
						if bounds.IsEmpty() {
							t.Skip("skip: journal is empty")
						}

						pos := Position(
							rapid.Uint64Range(
								uint64(bounds.Begin),
								uint64(bounds.End-1),
							).Draw(t, "pos"),
						)

						rec := rapid.String().Draw(t, "rec")

						err := j.Append(t.Context(), pos, []byte(rec))
						if !IsConflict(err) {
							t.Fatalf("unexpected error: got %q, want IsConflict(err) == true", err)
						}

						t.Logf("induced conflict appending at position %d, bounds are still %s", pos, bounds)
					},
					"Append (conflict with truncated record)": func(t *rapid.T) {
						if bounds.Begin == 0 {
							t.Skip("skip: no records have been truncated")
						}

						pos := Position(
							rapid.Uint64Range(
								uint64(0),
								uint64(bounds.Begin-1),
							).Draw(t, "pos"),
						)

						rec := rapid.String().Draw(t, "rec")

						err := j.Append(t.Context(), pos, []byte(rec))
						if !IsConflict(err) {
							t.Fatalf("unexpected error: got %q, want IsConflict(err) == true", err)
						}

						t.Logf("induced conflict appending at position %d, bounds are still %s", pos, bounds)
					},
					"Truncate (some)": func(t *rapid.T) {
						if bounds.Len() < 2 {
							t.Skip("skip: need at least 2 records")
						}

						pos := Position(
							rapid.Uint64Range(
								uint64(bounds.Begin),
								uint64(bounds.End-1),
							).Draw(t, "pos"),
						)

						err := j.Truncate(t.Context(), pos)
						if err != nil {
							t.Fatalf("unable to truncate records before position %d: %s", pos, err)
						}

						bounds.Begin = pos

						t.Logf("truncated records before position %d, bounds are now %s", pos, bounds)
					},
					"Truncate (all)": func(t *rapid.T) {
						if bounds.IsEmpty() {
							t.Skip("skip: journal is empty")
						}

						err := j.Truncate(t.Context(), bounds.End)
						if err != nil {
							t.Fatalf("unable to truncate records before position %d: %s", bounds.End, err)
						}

						bounds.Begin = bounds.End

						t.Logf("truncated records before position %d, bounds are now %s", bounds.End, bounds)
					},
					"Truncate (already truncated)": func(t *rapid.T) {
						if bounds.Begin == 0 {
							t.Skip("skip: no records have been truncated")
						}

						pos := Position(
							rapid.Uint64Max(
								uint64(bounds.Begin),
							).Draw(t, "pos"),
						)

						err := j.Truncate(t.Context(), pos)
						if err != nil {
							t.Fatalf("unable to truncate records before position %d: %s", pos, err)
						}

						t.Logf("truncated records before position %d, bounds are still %s", pos, bounds)
					},
				},
			)
		})
	})
}

// appendRecords appends records to j.
func appendRecords(
	t *testing.T,
	j BinaryJournal,
	n int,
) [][]byte {
	var records [][]byte

	for pos := Position(0); pos < Position(n); pos++ {
		rec := []byte(
			fmt.Sprintf("<record-%d>", pos),
		)

		records = append(records, rec)

		if err := j.Append(t.Context(), pos, rec); err != nil {
			t.Fatal(err)
		}
	}

	return records
}
