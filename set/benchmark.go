package set

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
)

// RunBenchmarks runs benchmarks against a [BinaryStore] implementation.
func RunBenchmarks(
	b *testing.B,
	store BinaryStore,
) {
	b.Run("Store", func(b *testing.B) {
		b.Run("Open", func(b *testing.B) {
			b.Run("existing set", func(b *testing.B) {
				var (
					name string
					set  BinarySet
				)

				xtesting.Benchmark(
					b,
					// SETUP
					func(ctx context.Context) error {
						name = xtesting.SequentialName("set")

						// pre-create the set
						set, err := store.Open(ctx, name)
						if err != nil {
							return err
						}
						return set.Close()
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context) (err error) {
						set, err = store.Open(ctx, name)
						return err
					},
					// AFTER EACH
					func(context.Context) error {
						return set.Close()
					},
				)
			})

			b.Run("new set", func(b *testing.B) {
				var (
					name string
					set  BinarySet
				)

				xtesting.Benchmark(
					b,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context) error {
						name = xtesting.SequentialName("set")
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context) (err error) {
						set, err = store.Open(ctx, name)
						return err
					},
					// AFTER EACH
					func(context.Context) error {
						return set.Close()
					},
				)
			})
		})
	})

	b.Run("Set", func(b *testing.B) {
		b.Run("Has", func(b *testing.B) {
			b.Run("non-existent value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinarySet) error {
						_, err := io.ReadFull(rand.Reader, value[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						_, err := set.Has(ctx, value[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing key", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, set BinarySet) error {
						if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
							return err
						}
						return set.Add(ctx, value[:])
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						_, err := set.Has(ctx, value[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Add", func(b *testing.B) {
			b.Run("non-existent value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinarySet) error {
						_, err := io.ReadFull(rand.Reader, value[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						return set.Add(ctx, value[:])
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, set BinarySet) error {
						if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
							return err
						}
						return set.Add(ctx, value[:])
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						return set.Add(ctx, value[:])
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("TryAdd", func(b *testing.B) {
			b.Run("non-existent value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinarySet) error {
						_, err := io.ReadFull(rand.Reader, value[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						_, err := set.TryAdd(ctx, value[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, set BinarySet) error {
						if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
							return err
						}
						return set.Add(ctx, value[:])
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						_, err := set.TryAdd(ctx, value[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Remove", func(b *testing.B) {
			b.Run("non-existent value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinarySet) error {
						_, err := io.ReadFull(rand.Reader, value[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						return set.Remove(ctx, value[:])
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, set BinarySet) error {
						if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
							return err
						}
						return set.Add(ctx, value[:])
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						return set.Remove(ctx, value[:])
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("TryRemove", func(b *testing.B) {
			b.Run("non-existent value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinarySet) error {
						_, err := io.ReadFull(rand.Reader, value[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						_, err := set.TryRemove(ctx, value[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing value", func(b *testing.B) {
				var value [32]byte

				benchmarkSet(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, set BinarySet) error {
						if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
							return err
						}
						return set.Add(ctx, value[:])
					},
					// BENCHMARKED CODE
					func(ctx context.Context, set BinarySet) error {
						_, err := set.TryRemove(ctx, value[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})
	})
}

func benchmarkSet(
	b *testing.B,
	store BinaryStore,
	setup func(context.Context, BinaryStore, BinarySet) error,
	before func(context.Context, BinarySet) error,
	fn func(context.Context, BinarySet) error,
	after func() error,
) {
	var set BinarySet

	xtesting.Benchmark(
		b,
		func(ctx context.Context) error {
			var err error
			set, err = store.Open(ctx, xtesting.SequentialName("set"))
			if err != nil {
				return err
			}

			b.Cleanup(func() {
				set.Close()
			})

			if setup != nil {
				return setup(ctx, store, set)
			}

			return nil
		},
		func(ctx context.Context) error {
			if before != nil {
				return before(ctx, set)
			}
			return nil
		},
		func(ctx context.Context) error {
			return fn(ctx, set)
		},
		func(context.Context) error {
			if after != nil {
				return after()
			}
			return nil
		},
	)
}
