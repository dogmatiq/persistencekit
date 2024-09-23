package kv

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
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
			b.Run("existing keyspace", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					store,
					// SETUP
					func(ctx context.Context, s BinaryStore) error {
						name = testx.UniqueName("keyspace")

						// pre-create the keyspace
						ks, err := s.Open(ctx, name)
						if err != nil {
							return err
						}
						return ks.Close()
					},
					// BEFORE EACH
					nil,
					// BENCHMARKED CODE
					func(ctx context.Context, s BinaryStore) (BinaryKeyspace, error) {
						return s.Open(ctx, name)
					},
					// AFTER EACH
					func(ks BinaryKeyspace) error {
						return ks.Close()
					},
				)
			})

			b.Run("new keyspace", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					store,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, BinaryStore) error {
						name = testx.UniqueName("keyspace")
						return nil
					},
					// BENCHMARKED CODE
					func(ctx context.Context, s BinaryStore) (BinaryKeyspace, error) {
						return s.Open(ctx, name)
					},
					// AFTER EACH
					func(ks BinaryKeyspace) error {
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
						_, err := ks.Get(ctx, key[:])
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
						return ks.Set(ctx, key[:], []byte("<value>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						_, err := ks.Get(ctx, key[:])
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
						return ks.Set(ctx, key[:], []byte("<value>"))
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
						return ks.Set(ctx, key[:], []byte("<value>"))
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
						return ks.Set(ctx, key[:], []byte("<value-1>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						return ks.Set(ctx, key[:], []byte("<value-2>"))
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
						return ks.Set(ctx, key[:], []byte("<value>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks BinaryKeyspace) error {
						return ks.Set(ctx, key[:], nil)
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
						if err := ks.Set(ctx, k, v); err != nil {
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
						func(context.Context, []byte, []byte) (bool, error) {
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

func benchmarkKeyspace(
	b *testing.B,
	store BinaryStore,
	setup func(context.Context, BinaryStore, BinaryKeyspace) error,
	before func(context.Context, BinaryKeyspace) error,
	fn func(context.Context, BinaryKeyspace) error,
	after func() error,
) {
	var keyspace BinaryKeyspace

	testx.Benchmark(
		b,
		func(ctx context.Context) error {
			var err error
			keyspace, err = store.Open(ctx, testx.UniqueName("keyspace"))
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
		func(ctx context.Context) error {
			if after != nil {
				return after()
			}
			return nil
		},
	)
}
