package journal

import "context"

// WithNameTransform returns a [Store] that uses x to transform the name of each
// journal within s.
//
// [Journal.Name] returns the untransformed name.
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

func (s *nameTransformStore[T]) Open(ctx context.Context, name string) (Journal[T], error) {
	j, err := s.Store.Open(ctx, s.transform(name))
	if err != nil {
		return nil, err
	}

	return &nameTransformJournal[T]{j, name}, nil
}

type nameTransformJournal[T any] struct {
	Journal[T]
	name string
}

func (j *nameTransformJournal[T]) Name() string {
	return j.name
}
