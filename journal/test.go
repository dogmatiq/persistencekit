package journal

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

// RunTests runs tests that confirm a journal implementation behaves correctly.
func RunTests(
	t *testing.T,
	newStore func(t *testing.T) Store,
) {
	type dependencies struct {
		Store       Store
		JournalName string
		Journal     Journal
	}

	setup := func(
		t *testing.T,
		newStore func(t *testing.T) Store,
	) (context.Context, *dependencies) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)

		deps := &dependencies{
			Store:       newStore(t),
			JournalName: fmt.Sprintf("<journal-%d>", journalCounter.Add(1)),
		}

		j, err := deps.Store.Open(ctx, deps.JournalName)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
		})

		deps.Journal = j

		return ctx, deps
	}

	t.Run("Store", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("allows a journal to be opened multiple times", func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				store := newStore(t)

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
					Desc                   string
					ExpectBegin, ExpectEnd Position
					Setup                  func(context.Context, *testing.T, Journal)
				}{
					{
						"empty",
						0, 0,
						func(ctx context.Context, t *testing.T, j Journal) {},
					},
					{
						"with records",
						0, 10,
						func(ctx context.Context, t *testing.T, j Journal) {
							appendRecords(ctx, t, j, 10)
						},
					},
					{
						"with truncated records",
						5, 10,
						func(ctx context.Context, t *testing.T, j Journal) {
							appendRecords(ctx, t, j, 10)
							if err := j.Truncate(ctx, 5); err != nil {
								t.Fatal(err)
							}
						},
					},
				}

				for _, c := range cases {
					t.Run(c.Desc, func(t *testing.T) {
						t.Parallel()

						ctx, deps := setup(t, newStore)

						c.Setup(ctx, t, deps.Journal)

						begin, end, err := deps.Journal.Bounds(ctx)
						if err != nil {
							t.Fatal(err)
						}

						if begin != c.ExpectBegin {
							t.Fatalf("unexpected begin position: got %d, want %d", begin, c.ExpectBegin)
						}

						if end != c.ExpectEnd {
							t.Fatalf("unexpected end position: got %d, want %d", end, c.ExpectEnd)
						}
					})
				}
			})
		})

		t.Run("Get", func(t *testing.T) {
			t.Parallel()

			t.Run("it returns ErrNotFound if there is no record at the given position", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				_, err := deps.Journal.Get(ctx, 1)
				if !errors.Is(err, ErrNotFound) {
					t.Fatal(err)
				}
			})

			t.Run("it returns the record if it exists", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				// Ensure we test with a position that becomes 2 digits long to
				// confirm that the implementation is not using a lexical sort.
				records := appendRecords(ctx, t, deps.Journal, 15)

				for i, want := range records {
					got, err := deps.Journal.Get(ctx, Position(i))
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

			t.Run("it does not return its internal byte slice", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				appendRecords(ctx, t, deps.Journal, 1)

				rec, err := deps.Journal.Get(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}

				rec[0] = 'X'

				got, err := deps.Journal.Get(ctx, 0)
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
		})

		t.Run("Range", func(t *testing.T) {
			t.Parallel()

			t.Run("calls the function for each record in the journal", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				want := appendRecords(ctx, t, deps.Journal, 15)

				var got [][]byte
				wantPos := Position(10)
				want = want[wantPos:]

				if err := deps.Journal.Range(
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

				ctx, deps := setup(t, newStore)

				appendRecords(ctx, t, deps.Journal, 2)

				called := false
				if err := deps.Journal.Range(
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

			t.Run("it returns ErrNotFound if the first record is truncated", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				records := appendRecords(ctx, t, deps.Journal, 5)
				retainPos := Position(len(records) - 1)

				err := deps.Journal.Truncate(ctx, retainPos)
				if err != nil {
					t.Fatal(err)
				}

				err = deps.Journal.Range(
					ctx,
					1,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						panic("unexpected call")
					},
				)

				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
				}
			})

			t.Run("it returns an error if a record is truncated during iteration", func(t *testing.T) {
				t.Skip() // TODO
				t.Parallel()

				ctx, deps := setup(t, newStore)

				appendRecords(ctx, t, deps.Journal, 5)

				err := deps.Journal.Range(
					ctx,
					0,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						return true, deps.Journal.Truncate(ctx, 5)
					},
				)

				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrNotFound)
				}
			})

			t.Run("it does not invoke the function with its internal byte slice", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				appendRecords(ctx, t, deps.Journal, 1)

				if err := deps.Journal.Range(
					ctx,
					0,
					func(ctx context.Context, pos Position, rec []byte) (bool, error) {
						rec[0] = 'X'

						return true, nil
					},
				); err != nil {
					t.Fatal(err)
				}

				got, err := deps.Journal.Get(ctx, 0)
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
		})

		t.Run("Append", func(t *testing.T) {
			t.Parallel()

			t.Run("it does not return an error if there is no record at the given position", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				if err := deps.Journal.Append(ctx, 0, []byte("<record>")); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it returns ErrConflict there is already a record at the given position", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				if err := deps.Journal.Append(ctx, 0, []byte("<prior>")); err != nil {
					t.Fatal(err)
				}

				want := []byte("<original>")
				if err := deps.Journal.Append(ctx, 1, want); err != nil {
					t.Fatal(err)
				}

				err := deps.Journal.Append(ctx, 1, []byte("<modified>"))

				if !errors.Is(err, ErrConflict) {
					t.Fatalf("unexpected error: got %q, want %q", err, ErrConflict)
				}

				got, err := deps.Journal.Get(ctx, 1)
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

				ctx, deps := setup(t, newStore)

				rec := []byte("<record>")

				if err := deps.Journal.Append(ctx, 0, rec); err != nil {
					t.Fatal(err)
				}

				rec[0] = 'X'

				got, err := deps.Journal.Get(ctx, 0)
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

				ctx, deps := setup(t, newStore)

				appendRecords(ctx, t, deps.Journal, 3)

				if err := deps.Journal.Truncate(ctx, 1); err != nil {
					t.Fatal(err)
				}

				got, _, err := deps.Journal.Bounds(ctx)
				if err != nil {
					t.Fatal(err)
				}

				want := Position(1)
				if got != want {
					t.Fatalf("unexpected begin position: got %d, want %d", got, want)
				}
			})

			t.Run("it truncates the journal when it has already been truncated", func(t *testing.T) {
				t.Parallel()

				ctx, deps := setup(t, newStore)

				appendRecords(ctx, t, deps.Journal, 3)

				if err := deps.Journal.Truncate(ctx, 1); err != nil {
					t.Fatal(err)
				}

				if err := deps.Journal.Truncate(ctx, 2); err != nil {
					t.Fatal(err)
				}

				got, _, err := deps.Journal.Bounds(ctx)
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

var journalCounter atomic.Uint64

// appendRecords appends records to j.
func appendRecords(
	ctx context.Context,
	t *testing.T,
	j Journal,
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
