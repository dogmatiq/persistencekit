package journal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// RunTests runs tests that confirm a journal implementation behaves correctly.
func RunTests(
	t *testing.T,
	store BinaryStore,
) {
	setup := func(t *testing.T) (context.Context, BinaryJournal) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)

		name := uniqueName()
		j, err := store.Open(ctx, name)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := j.Close(); err != nil {
				t.Error(err)
			}
		})

		return ctx, j
	}

	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows a journal to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				j1, err := store.Open(ctx, "<journal>")
				if err != nil {
					t.Fatal(err)
				}
				defer j1.Close()

				j2, err := store.Open(ctx, "<journal>")
				if err != nil {
					t.Fatal(err)
				}
				defer j2.Close()

				want := []byte("<record>")
				if err := j1.Append(ctx, 0, want); err != nil {
					t.Fatal(err)
				}

				got, err := j2.Get(ctx, 0)
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
					Name                   string
					ExpectBegin, ExpectEnd Position
					Setup                  func(context.Context, *testing.T, BinaryJournal)
				}{
					{
						"empty",
						0, 0,
						func(ctx context.Context, t *testing.T, j BinaryJournal) {},
					},
					{
						"with records",
						0, 10,
						func(ctx context.Context, t *testing.T, j BinaryJournal) {
							appendRecords(ctx, t, j, 10)
						},
					},
					{
						"with some records truncated",
						5, 10,
						func(ctx context.Context, t *testing.T, j BinaryJournal) {
							appendRecords(ctx, t, j, 10)
							if err := j.Truncate(ctx, 5); err != nil {
								t.Fatal(err)
							}
						},
					},
					{
						"with all records truncated",
						10, 10,
						func(ctx context.Context, t *testing.T, j BinaryJournal) {
							appendRecords(ctx, t, j, 10)
							if err := j.Truncate(ctx, 10); err != nil {
								t.Fatal(err)
							}
						},
					},
				}

				for _, c := range cases {
					t.Run(c.Name, func(t *testing.T) {
						t.Parallel()

						ctx, j := setup(t)

						c.Setup(ctx, t, j)

						begin, end, err := j.Bounds(ctx)
						if err != nil {
							t.Fatal(err)
						}

						if begin != c.ExpectBegin || end != c.ExpectEnd {
							t.Fatalf(
								"unexpected bounds: got [%d, %d), want [%d, %d)",
								begin, end,
								c.ExpectBegin, c.ExpectEnd,
							)
						}
					})
				}
			})
		})

		t.Run("Get", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns ErrNotFound if there is no record at the given position", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				_, err := j.Get(ctx, 1)
				if !errors.Is(err, ErrNotFound) {
					t.Fatal(err)
				}
			})

			t.Run("it returns the record if it exists", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				// Ensure we test with a position that becomes 2 digits long to
				// confirm that the implementation is not using a lexical sort.
				records := appendRecords(ctx, t, j, 15)

				for i, want := range records {
					got, err := j.Get(ctx, Position(i))
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

			t.Run("it does not return truncated records", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				const recordCount = 5
				const truncateBefore = 3
				records := appendRecords(ctx, t, j, recordCount)

				err := j.Truncate(ctx, truncateBefore)
				if err != nil {
					t.Fatal(err)
				}

				for pos, want := range records {
					pos := Position(pos)

					if pos < truncateBefore {
						if _, err := j.Get(ctx, pos); err != ErrNotFound {
							t.Fatalf("unexpected error at position %d: got %q, want %q", pos, err, ErrNotFound)
						}
					} else {
						got, err := j.Get(ctx, pos)
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

				ctx, j := setup(t)

				records := appendRecords(ctx, t, j, 5)

				err := j.Truncate(ctx, 5)
				if err != nil {
					t.Fatal(err)
				}

				for i := range records {
					pos := Position(i)
					if _, err := j.Get(ctx, pos); err != ErrNotFound {
						t.Fatalf("unexpected error at position %d: got %q, want %q", i, err, ErrNotFound)
					}
				}
			})

			t.Run("it does not return its internal byte slice", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 1)

				rec, err := j.Get(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}

				rec[0] = 'X'

				got, err := j.Get(ctx, 0)
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

				ctx, j := setup(t)

				_, err := j.Get(ctx, math.MaxUint64)
				if !errors.Is(err, ErrNotFound) {
					t.Fatal(err)
				}
			})
		})

		t.Run("Range", func(t *testing.T) {
			t.Parallel()

			t.Run("calls the function for each record in the journal", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				want := appendRecords(ctx, t, j, 15)

				var got [][]byte
				wantPos := Position(10)
				want = want[wantPos:]

				if err := j.Range(
					ctx,
					wantPos,
					func(ctx context.Context, gotPos Position, rec []byte) (bool, error) {
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

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 2)

				called := false
				if err := j.Range(
					ctx,
					0,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
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

			t.Run("it returns ErrNotFound if journal is empty", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				err := j.Range(
					ctx,
					0,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						panic("unexpected call")
					},
				)

				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
				}
			})

			t.Run("it does not range over truncated records", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				const recordCount = 5
				const truncateBefore = 3
				records := appendRecords(ctx, t, j, recordCount)

				err := j.Truncate(ctx, truncateBefore)
				if err != nil {
					t.Fatal(err)
				}

				for pos, want := range records {
					pos := Position(pos)

					if pos < truncateBefore {
						if err := j.Range(
							ctx,
							pos,
							func(ctx context.Context, pos Position, rec []byte) (bool, error) {
								panic("unexpected call")
							},
						); !errors.Is(err, ErrNotFound) {
							t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
						}
					} else {
						if err := j.Range(
							ctx,
							pos,
							func(ctx context.Context, pos Position, got []byte) (bool, error) {
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

				ctx, j := setup(t)

				records := appendRecords(ctx, t, j, 5)

				err := j.Truncate(ctx, 5)
				if err != nil {
					t.Fatal(err)
				}

				for pos := range records {
					pos := Position(pos)

					if err := j.Range(
						ctx,
						pos,
						func(ctx context.Context, pos Position, rec []byte) (bool, error) {
							panic("unexpected call")
						},
					); !errors.Is(err, ErrNotFound) {
						t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
					}
				}
			})

			t.Run("it returns an error if a record is truncated during iteration", func(t *testing.T) {
				t.Skip() // TODO
				t.Parallel()

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 5)

				err := j.Range(
					ctx,
					0,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						return true, j.Truncate(ctx, 5)
					},
				)

				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
				}
			})

			t.Run("it does not invoke the function with its internal byte slice", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 1)

				if err := j.Range(
					ctx,
					0,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						rec[0] = 'X'

						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				got, err := j.Get(ctx, 0)
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

				ctx, j := setup(t)

				err := j.Range(
					ctx,
					math.MaxUint64,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						panic("unexpected call")
					},
				)

				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
				}
			})
		})

		t.Run("Append", func(t *testing.T) {
			t.Parallel()

			t.Run("it does not return an error if there is no record at the given position", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				if err := j.Append(ctx, 0, []byte("<record>")); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it returns ErrConflict there is already a record at the given position", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				if err := j.Append(ctx, 0, []byte("<prior>")); err != nil {
					t.Fatal(err)
				}

				want := []byte("<original>")
				if err := j.Append(ctx, 1, want); err != nil {
					t.Fatal(err)
				}

				err := j.Append(ctx, 1, []byte("<modified>"))

				if !errors.Is(err, ErrConflict) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrConflict)
				}

				got, err := j.Get(ctx, 1)
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

			t.Run("it does not keep a reference to the record slice", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				rec := []byte("<record>")

				if err := j.Append(ctx, 0, rec); err != nil {
					t.Fatal(err)
				}

				rec[0] = 'X'

				got, err := j.Get(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}

				want := []byte("<record>")
				if !bytes.Equal(got, want) {
					t.Fatalf("unexpected record: got %q, want %q", string(got), string(want))
				}
			})
		})

		t.Run("Truncate", func(t *testing.T) {
			t.Parallel()

			t.Run("it truncates the journal", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 3)

				if err := j.Truncate(ctx, 1); err != nil {
					t.Fatal(err)
				}

				got, _, err := j.Bounds(ctx)
				if err != nil {
					t.Fatal(err)
				}

				want := Position(1)
				if got != want {
					t.Fatalf("unexpected begin position: got %d, want %d", got, want)
				}
			})

			t.Run("it allows truncating all records", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 3)

				if err := j.Truncate(ctx, 3); err != nil {
					t.Fatal(err)
				}

				begin, end, err := j.Bounds(ctx)
				if err != nil {
					t.Fatal(err)
				}

				want := Position(3)

				if begin != want || end != want {
					t.Fatalf(
						"unexpected bounds: got [%d, %d), want [%d, %d)",
						begin, end,
						want, want,
					)
				}
			})

			t.Run("it truncates the journal when it has already been truncated", func(t *testing.T) {
				t.Parallel()

				ctx, j := setup(t)

				appendRecords(ctx, t, j, 3)

				if err := j.Truncate(ctx, 1); err != nil {
					t.Fatal(err)
				}

				if err := j.Truncate(ctx, 2); err != nil {
					t.Fatal(err)
				}

				got, _, err := j.Bounds(ctx)
				if err != nil {
					t.Fatal(err)
				}

				want := Position(2)
				if got != want {
					t.Fatalf("unexpected begin position: got %d, want %d", got, want)
				}
			})
		})
	})
}

var nameCounter atomic.Uint64

func uniqueName() string {
	return fmt.Sprintf("<journal-%d>", nameCounter.Add(1))
}

// appendRecords appends records to j.
func appendRecords(
	ctx context.Context,
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

		if err := j.Append(ctx, pos, rec); err != nil {
			t.Fatal(err)
		}
	}

	return records
}
