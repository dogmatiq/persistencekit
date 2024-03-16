package kv

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
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
			b.Run("existing keyspace", func(b *testing.B) {
				var name string

				benchmarkStore(
					b,
					newStore,
					// SETUP
					func(ctx context.Context, store Store) error {
						name = uniqueName()

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
					func(ctx context.Context, store Store) (Keyspace, error) {
						return store.Open(ctx, name)
					},
					// AFTER EACH
					func(ks Keyspace) error {
						return ks.Close()
					},
				)
			})

			b.Run("new keyspace", func(b *testing.B) {
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
					func(ctx context.Context, store Store) (Keyspace, error) {
						return store.Open(ctx, name)
					},
					// AFTER EACH
					func(ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, Keyspace) error {
						_, err := io.ReadFull(rand.Reader, key[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks Keyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, Keyspace) error {
						_, err := io.ReadFull(rand.Reader, key[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks Keyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(context.Context, Keyspace) error {
						_, err := io.ReadFull(rand.Reader, key[:])
						return err
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks Keyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value-1>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
					newStore,
					// SETUP
					nil,
					// BEFORE EACH
					func(ctx context.Context, ks Keyspace) error {
						if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
							return err
						}
						return ks.Set(ctx, key[:], []byte("<value>"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
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
				newStore,
				// SETUP
				func(ctx context.Context, _ Store, ks Keyspace) error {
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
				func(ctx context.Context, ks Keyspace) error {
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

func benchmarkKeyspace(
	b *testing.B,
	newStore func(b *testing.B) Store,
	setup func(context.Context, Store, Keyspace) error,
	before func(context.Context, Keyspace) error,
	fn func(context.Context, Keyspace) error,
	after func() error,
) {
	var (
		store    Store
		keyspace Keyspace
	)

	benchmark.Run(
		b,
		func(ctx context.Context) error {
			store = newStore(b)

			var err error
			keyspace, err = store.Open(ctx, uniqueName())
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
