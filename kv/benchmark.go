package kv

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"time"
)

const iterationThreshold = 1_000_000

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
						name = uniqueKeyspaceName()

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
						name = uniqueKeyspaceName()
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
						return ks.Set(ctx, key[:], []byte("value"))
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
						return ks.Set(ctx, key[:], []byte("value"))
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
						return ks.Set(ctx, key[:], []byte("value"))
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
						return ks.Set(ctx, key[:], []byte("value-1"))
					},
					// BENCHMARKED CODE
					func(ctx context.Context, ks Keyspace) error {
						return ks.Set(ctx, key[:], []byte("value-2"))
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
						return ks.Set(ctx, key[:], []byte("value"))
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

		b.Run("Range (10k pairs)", func(b *testing.B) {
			benchmarkKeyspace(
				b,
				newStore,
				// SETUP
				func(ctx context.Context, _ Store, ks Keyspace) error {
					for i := 0; i < 10000; i++ {
						k := []byte(fmt.Sprintf("key-%d", i))
						v := []byte(fmt.Sprintf("value-%d", i))

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
						func(ctx context.Context, k, v []byte) (bool, error) {
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
	beforeEach func(context.Context, Store) error,
	run func(context.Context, Store) (T, error),
	afterEach func(T) error,
) {
	b.StopTimer()

	if b.N >= iterationThreshold {
		b.Skipf("too many iterations (%d); benchmarked code is likely too fast to measure meaningfully", b.N)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := newStore(b)

	if setup != nil {
		err := setup(ctx, store)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		if beforeEach != nil {
			if err := beforeEach(ctx, store); err != nil {
				cancel()
				b.Fatal(err)
			}
		}

		b.StartTimer()
		result, err := run(ctx, store)
		b.StopTimer()

		cancel()

		if err != nil {
			b.Fatal(err)
		}

		if afterEach != nil {
			if err := afterEach(result); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkKeyspace(
	b *testing.B,
	newStore func(b *testing.B) Store,
	setup func(context.Context, Store, Keyspace) error,
	beforeEach func(context.Context, Keyspace) error,
	run func(context.Context, Keyspace) error,
	afterEach func() error,
) {
	b.StopTimer()

	if b.N >= iterationThreshold {
		b.Skipf("too many iterations (%d); benchmarked code is likely too fast to measure meaningfully", b.N)
	}

	store := newStore(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ks, err := store.Open(ctx, uniqueKeyspaceName())
	if err != nil {
		b.Fatal(err)
	}

	if setup != nil {
		err := setup(ctx, store, ks)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err != nil {
			cancel()
		}

		if beforeEach != nil {
			if err := beforeEach(ctx, ks); err != nil {
				cancel()
				b.Fatal(err)
			}
		}

		b.StartTimer()
		err := run(ctx, ks)
		b.StopTimer()

		cancel()

		if err != nil {
			b.Fatal(err)
		}

		if afterEach != nil {
			if err := afterEach(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
