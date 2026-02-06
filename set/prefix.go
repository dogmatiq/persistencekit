package set

import "context"

// WithNamePrefix returns a [Store] that adds the given prefix to all set
// names.
func WithNamePrefix[T any](store Store[T], prefix string) Store[T] {
	return prefixedStore[T]{store, prefix}
}

// prefixedStore is a [Store] that adds a prefix to all set names.
type prefixedStore[T any] struct {
	Store[T]
	prefix string
}

func (s prefixedStore[T]) Open(ctx context.Context, name string) (Set[T], error) {
	ks, err := s.Store.Open(ctx, s.prefix+name)
	if err != nil {
		return nil, err
	}

	return prefixedSet[T]{ks, name}, nil
}

// prefixedSet is a [Keyspace] opened by a [prefixedStore].
type prefixedSet[T any] struct {
	Set[T]
	name string
}

func (ks prefixedSet[T]) Name() string {
	return ks.name
}
