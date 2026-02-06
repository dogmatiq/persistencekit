package kv

import "context"

// WithNameTransform returns a [Store] that uses x to transform the name of each
// keyspace within s.
//
// [Keyspace.Name] returns the untransformed name.
func WithNameTransform[K, V any](
	s Store[K, V],
	x func(string) string,
) Store[K, V] {
	return &nameTransformStore[K, V]{s, x}
}

type nameTransformStore[K, V any] struct {
	Store[K, V]
	transform func(string) string
}

func (s *nameTransformStore[K, V]) Open(ctx context.Context, name string) (Keyspace[K, V], error) {
	ks, err := s.Store.Open(ctx, s.transform(name))
	if err != nil {
		return nil, err
	}

	return &nameTransformKeyspace[K, V]{ks, name}, nil
}

type nameTransformKeyspace[K, V any] struct {
	Keyspace[K, V]
	name string
}

func (ks *nameTransformKeyspace[K, V]) Name() string {
	return ks.name
}
