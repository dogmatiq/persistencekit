package set

import "context"

// WithNameTransform returns a [Store] that uses x to transform the name of each
// set within s.
//
// [Set.Name] returns the untransformed name.
func WithNameTransform[T any](
	s Store[T],
	x func(string) string,
) Store[T] {
	return &nameTransformStore[T]{s, x}
}

type nameTransformStore[T any] struct {
	Store[T]
	transform func(string) string
}

func (s *nameTransformStore[T]) Open(ctx context.Context, name string) (Set[T], error) {
	ks, err := s.Store.Open(ctx, s.transform(name))
	if err != nil {
		return nil, err
	}

	return &nameTransformSet[T]{ks, name}, nil
}

type nameTransformSet[T any] struct {
	Set[T]
	name string
}

func (s *nameTransformSet[T]) Name() string {
	return s.name
}
