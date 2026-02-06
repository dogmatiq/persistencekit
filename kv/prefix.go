package kv

import "context"

// WithNamePrefix returns a [Store] that adds the given prefix to all keyspace
// names.
func WithNamePrefix[K, V any](store Store[K, V], prefix string) Store[K, V] {
	return prefixedStore[K, V]{store, prefix}
}

// prefixedStore is a [Store] that adds a prefix to all keyspace names.
type prefixedStore[K, V any] struct {
	Store[K, V]
	prefix string
}

func (s prefixedStore[K, V]) Open(ctx context.Context, name string) (Keyspace[K, V], error) {
	ks, err := s.Store.Open(ctx, s.prefix+name)
	if err != nil {
		return nil, err
	}

	return prefixedKeyspace[K, V]{ks, name}, nil
}

// prefixedKeyspace is a [Keyspace] opened by a [prefixedStore].
type prefixedKeyspace[K, V any] struct {
	Keyspace[K, V]
	name string
}

func (ks prefixedKeyspace[K, V]) Name() string {
	return ks.name
}
