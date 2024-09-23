package journal

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/dogmatiq/persistencekit/internal/testx"
)

// RunBenchmarks runs benchmarks against a [BinaryStore] implementation.
func RunBenchmarks(
	b *testing.B,
	store BinaryStore,
) {
	b.Run("Store", func(b *testing.B) {
		b.Run("Open", func(b *testing.B) {
			b.Run("existing journal", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					store,
					// SETUP
					func(ctx context.Context, s BinaryStore) error {
						name = testx.UniqueName("journal")

						// pre-create the journal
						ks, err := s.Open(ctx, name)
						if err != nil {
							return err
						}
						return ks.Close()
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context, s BinaryStore) (BinaryJournal, error) {
						return s.Open(ctx, name)
					},
					// AFTER EACH
					func(j BinaryJournal) error {
						return j.Close()
					},
				)
			})

			b.Run("new journal", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinaryStore) error {
						name = testx.UniqueName("journal")
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, s BinaryStore) (BinaryJournal, error) {
						return s.Open(ctx, name)
					},
					// AFTER EACH
					func(j BinaryJournal) error {
						return j.Close()
					},
				)
			})
		})
	})

	b.Run("Journal", func(b *testing.B) {
		b.Run("Bounds", func(b *testing.B) {
			b.Run("empty journal", func(b *testing.B) {
				benchmarkJournal(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context, j BinaryJournal) error {
						_, err := j.Bounds(ctx)
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("non-empty journal", func(b *testing.B) {
				benchmarkJournal(
					b,
					store,
					// SETUP
					func(ctx context.Context, s BinaryStore, j BinaryJournal) error {
						for pos := Position(0); pos < 10000; pos++ {
							rec := []byte(fmt.Sprintf("<record-%d>", pos))
							if err := j.Append(ctx, pos, rec); err != nil {
								return err
							}
						}
						return nil
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context, j BinaryJournal) error {
						_, err := j.Bounds(ctx)
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("truncated journal", func(b *testing.B) {
				benchmarkJournal(
					b,
					store,
					// SETUP
					func(ctx context.Context, s BinaryStore, j BinaryJournal) error {
						for pos := Position(0); pos < 10000; pos++ {
							rec := []byte(fmt.Sprintf("<record-%d>", pos))
							if err := j.Append(ctx, pos, rec); err != nil {
								return err
							}
						}

						return j.Truncate(ctx, 5000)
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context, j BinaryJournal) error {
						_, err := j.Bounds(ctx)
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Get", func(b *testing.B) {
			b.Run("non-existent record", func(b *testing.B) {
				var pos Position

				benchmarkJournal(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinaryJournal) error {
						pos = Position(rand.Uint64())
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, j BinaryJournal) error {
						_, err := j.Get(ctx, pos)
						return IgnoreNotFound(err)
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing record", func(b *testing.B) {
				var pos Position

				benchmarkJournal(
					b,
					store,
					// SETUP
					func(ctx context.Context, _ BinaryStore, j BinaryJournal) error {
						for pos := Position(0); pos < 10000; pos++ {
							rec := []byte(fmt.Sprintf("<record-%d>", pos))
							if err := j.Append(ctx, pos, rec); err != nil {
								return err
							}
						}
						return nil
					},
					// BEFORE EACH
					func(ctx context.Context, j BinaryJournal) error {
						pos = Position(rand.Uint64() % 10000)
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, j BinaryJournal) error {
						_, err := j.Get(ctx, pos)
						return IgnoreNotFound(err)
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
				store,
				// SETUP
				nil,
				// BEFORE EACH
				nil,
				// BENCHMARKED CODE
				func(ctx context.Context, j BinaryJournal) error {
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
				store,
				// SETUP
				func(ctx context.Context, _ BinaryStore, j BinaryJournal) error {
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
				func(ctx context.Context, j BinaryJournal) error {
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
				store,
				// SETUP
				func(ctx context.Context, _ BinaryStore, j BinaryJournal) error {
					rec := []byte("<record>")
					for pos := 0; pos <= b.N; pos++ {
						if err := j.Append(ctx, Position(pos), rec); err != nil {
							return err
						}
					}
					return nil
				},
				// BEFORE EACH
				nil,
				// BENCHMARKED CODE
				func(ctx context.Context, j BinaryJournal) error {
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
	store BinaryStore,
	setup func(context.Context, BinaryStore) error,
	before func(context.Context, BinaryStore) error,
	fn func(context.Context, BinaryStore) (T, error),
	after func(T) error,
) {
	var result T

	testx.Benchmark(
		b,
		func(ctx context.Context) error {
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
	store BinaryStore,
	setup func(context.Context, BinaryStore, BinaryJournal) error,
	before func(context.Context, BinaryJournal) error,
	fn func(context.Context, BinaryJournal) error,
	after func() error,
) {
	var journ BinaryJournal

	testx.Benchmark(
		b,
		func(ctx context.Context) error {
			var err error
			journ, err = store.Open(ctx, testx.UniqueName("journal"))
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
