package journal

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/dogmatiq/persistencekit/internal/benchmark"
)

// RunBenchmarks runs benchmarks against a [Store] implementation.
func RunBenchmarks(
	b *testing.B,
	newStore func(b *testing.B) Store,
) {
	b.Run("Store", func(b *testing.B) {
		b.Run("Open", func(b *testing.B) {
			b.Run("existing journal", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					newStore,
					// SETUP
					func(ctx context.Context, store Store) error {
						name = uniqueName()

						// pre-create the journal
						ks, err := store.Open(ctx, name)
						if err != nil {
							return err
						}
						return ks.Close()
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context, store Store) (Journal, error) {
						return store.Open(ctx, name)
					},
					// AFTER EACH
					func(j Journal) error {
						return j.Close()
					},
				)
			})

			b.Run("new journal", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, Store) error {
						name = uniqueName()
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, store Store) (Journal, error) {
						return store.Open(ctx, name)
					},
					// AFTER EACH
					func(j Journal) error {
						return j.Close()
					},
				)
			})
		})
	})

	b.Run("Journal", func(b *testing.B) {
		b.Run("Get", func(b *testing.B) {
			b.Run("non-existent record", func(b *testing.B) {
				var pos Position

				benchmarkJournal(
					b,
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, Journal) error {
						pos = Position(
							rand.Int63n(
								int64(MaxPosition),
							),
						)
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, j Journal) error {
						_, err := j.Get(ctx, pos)
						if err == ErrNotFound {
							return nil
						}
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing record", func(b *testing.B) {
				var pos Position

				benchmarkJournal(
					b,
					newStore,
					// SETUP
					func(ctx context.Context, _ Store, j Journal) error {
						for pos := Position(0); pos < 10000; pos++ {
							rec := []byte(fmt.Sprintf("<record-%d>", pos))
							if err := j.Append(ctx, pos, rec); err != nil {
								return err
							}
						}
						return nil
					},
					// BEFORE EACH
					func(ctx context.Context, j Journal) error {
						pos = Position(rand.Uint64() % 10000)
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, j Journal) error {
						_, err := j.Get(ctx, pos)
						if err == ErrNotFound {
							return nil
						}
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Append", func(b *testing.B) {
			var pos Position

			benchmarkJournal(
				b,
				newStore,
				// SETUP
				nil,
				// BEFORE EACH
				nil,
				// BENCHMARKED CODE
				func(ctx context.Context, j Journal) error {
					return j.Append(ctx, pos, []byte("<value>"))
				},
				// AFTER EACH
				func() error {
					pos++
					return nil
				},
			)
		})

		b.Run("Range (3k records)", func(b *testing.B) {
			benchmarkJournal(
				b,
				newStore,
				// SETUP
				func(ctx context.Context, _ Store, j Journal) error {
					rec := []byte("<record>")
					for pos := Position(0); pos < 3000; pos++ {
						if err := j.Append(ctx, pos, rec); err != nil {
							return err
						}
					}
					return nil
				},
				// BEFORE EACH
				nil,
				// BENCHMARKED CODE
				func(ctx context.Context, j Journal) error {
					return j.Range(
						ctx,
						0,
						func(context.Context, Position, []byte) (bool, error) {
							return true, nil
						},
					)
				},
				// AFTER EACH
				nil,
			)
		})

		b.Run("Truncate (1 record)", func(b *testing.B) {
			var pos Position

			benchmarkJournal(
				b,
				newStore,
				// SETUP
				func(ctx context.Context, store Store, j Journal) error {
					rec := []byte("<record>")
					for pos := 0; pos < b.N; pos++ {
						if err := j.Append(ctx, Position(pos), rec); err != nil {
							return err
						}
					}
					return nil
				},
				// BEFORE EACH
				nil,
				// BENCHMARKED CODE
				func(ctx context.Context, j Journal) error {
					pos++
					return j.Truncate(ctx, pos)
				},
				// AFTER EACH
				nil,
			)
		})
	})
}

func benchmarkStore[T any](
	b *testing.B,
	newStore func(b *testing.B) Store,
	setup func(context.Context, Store) error,
	before func(context.Context, Store) error,
	fn func(context.Context, Store) (T, error),
	after func(T) error,
) {
	var (
		store  Store
		result T
	)

	benchmark.Run(
		b,
		func(ctx context.Context) error {
			store = newStore(b)

			if setup != nil {
				return setup(ctx, store)
			}

			return nil
		},
		func(ctx context.Context) error {
			if before != nil {
				return before(ctx, store)
			}
			return nil
		},
		func(ctx context.Context) error {
			var err error
			result, err = fn(ctx, store)
			return err
		},
		func(ctx context.Context) error {
			if after != nil {
				return after(result)
			}
			return nil
		},
	)
}

func benchmarkJournal(
	b *testing.B,
	newStore func(b *testing.B) Store,
	setup func(context.Context, Store, Journal) error,
	before func(context.Context, Journal) error,
	fn func(context.Context, Journal) error,
	after func() error,
) {
	var (
		store Store
		journ Journal
	)

	benchmark.Run(
		b,
		func(ctx context.Context) error {
			store = newStore(b)

			var err error
			journ, err = store.Open(ctx, uniqueName())
			if err != nil {
				return err
			}

			b.Cleanup(func() {
				journ.Close()
			})

			if setup != nil {
				return setup(ctx, store, journ)
			}

			return nil
		},
		func(ctx context.Context) error {
			if before != nil {
				return before(ctx, journ)
			}
			return nil
		},
		func(ctx context.Context) error {
			return fn(ctx, journ)
		},
		func(ctx context.Context) error {
			if after != nil {
				return after()
			}
			return nil
		},
	)
}