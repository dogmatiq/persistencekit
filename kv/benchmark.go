package kv

import (
	"context"
	"crypto/rand"
	"fmt"
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
			b.Run("existing keyspace", func(b *testing.B) {
				var (
					name string
					ks   BinaryKeyspace
				)

				xtesting.Benchmark(
					b,
					// SETUP
					func(ctx context.Context) error {
						name = xtesting.SequentialName("keyspace")

						// pre-create the keyspace
						ks, err := store.Open(ctx, name)
						if err != nil {
							return err
						}
						return ks.Close()
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context) (err error) {
						ks, err = store.Open(ctx, name)
						return err
					},
					// AFTER EACH
					func(context.Context) error {
						return ks.Close()
					},
				)
			})

			b.Run("new keyspace", func(b *testing.B) {
				var (
					name string
					ks   BinaryKeyspace
				)

				xtesting.Benchmark(
					b,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context) error {
						name = xtesting.SequentialName("keyspace")
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context) (err error) {
						ks, err = store.Open(ctx, name)
						return err
					},
					// AFTER EACH
					func(context.Context) error {
						return ks.Close()
					},
				)
			})
		})
	})

	b.Run("Keyspace", func(b *testing.B) {
		b.Run("Get", func(b *testing.B) {
			b.Run("non-existent key", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinaryKeyspace) error {
						_, err := io.ReadFull(rand.Reader, key[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						_, _, err := ks.Get(ctx, key[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing key", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks BinaryKeyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value>"), 0)
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						_, _, err := ks.Get(ctx, key[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Has", func(b *testing.B) {
			b.Run("non-existent key", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinaryKeyspace) error {
						_, err := io.ReadFull(rand.Reader, key[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						_, err := ks.Has(ctx, key[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing key", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks BinaryKeyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value>"), 0)
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						_, err := ks.Has(ctx, key[:])
						return err
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Set", func(b *testing.B) {
			b.Run("non-existent key", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinaryKeyspace) error {
						_, err := io.ReadFull(rand.Reader, key[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						return ks.Set(ctx, key[:], []byte("<value>"), 0)
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing key", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks BinaryKeyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value-1>"), 0)
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						return ks.Set(ctx, key[:], []byte("<value-2>"), 1)
					},
					// AFTER EACH
					nil,
				)
			})

			b.Run("existing key set to empty", func(b *testing.B) {
				var key [32]byte

				benchmarkKeyspace(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks BinaryKeyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value>"), 0)
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						return ks.Set(ctx, key[:], nil, 1)
					},
					// AFTER EACH
					nil,
				)
			})
		})

		b.Run("Range (3k pairs)", func(b *testing.B) {
			benchmarkKeyspace(
				b,
				store,
				// SETUP
				func(ctx context.Context, _ BinaryStore, ks BinaryKeyspace) error {
					for i := 0; i < 3000; i++ {
						k := []byte(fmt.Sprintf("<key-%d>", i))
						v := []byte("<value>")
						if err := ks.Set(ctx, k, v, 0); err != nil {
							return err
						}
					}
					return nil
				},
				// BEFORE EACH
				nil,
				// BENCHMARKED CODE
				func(ctx context.Context, ks BinaryKeyspace) error {
					return ks.Range(
						ctx,
						func(_ context.Context, _, _ []byte, _ Revision) (bool, error) {
							return true, nil
						},
					)
				},
				// AFTER EACH
				nil,
			)
		})
	})
}

func benchmarkKeyspace(
	b *testing.B,
	store BinaryStore,
	setup func(context.Context, BinaryStore, BinaryKeyspace) error,
	before func(context.Context, BinaryKeyspace) error,
	fn func(context.Context, BinaryKeyspace) error,
	after func() error,
) {
	var keyspace BinaryKeyspace

	xtesting.Benchmark(
		b,
		func(ctx context.Context) error {
			var err error
			keyspace, err = store.Open(ctx, xtesting.SequentialName("keyspace"))
			if err != nil {
				return err
			}

			b.Cleanup(func() {
				keyspace.Close()
			})

			if setup != nil {
				return setup(ctx, store, keyspace)
			}

			return nil
		},
		func(ctx context.Context) error {
			if before != nil {
				return before(ctx, keyspace)
			}
			return nil
		},
		func(ctx context.Context) error {
			return fn(ctx, keyspace)
		},
		func(context.Context) error {
			if after != nil {
				return after()
			}
			return nil
		},
	)
}
