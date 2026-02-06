package journal

import "context"

// WithNamePrefix returns a [Store] that adds the given prefix to all journal
// names.
func WithNamePrefix[T any](store Store[T], prefix string) Store[T] {
	return prefixedStore[T]{store, prefix}
}

// prefixedStore is a [Store] that adds a prefix to all journal names.
type prefixedStore[T any] struct {
	Store[T]
	prefix string
}

func (s prefixedStore[T]) Open(ctx context.Context, name string) (Journal[T], error) {
	j, err := s.Store.Open(ctx, s.prefix+name)
	if err != nil {
		return nil, err
	}

	return prefixedJournal[T]{j, name}, nil
}

// prefixedJournal is a [Journal] opened by a [prefixedStore].
type prefixedJournal[T any] struct {
	Journal[T]
	name string
}

func (j prefixedJournal[T]) Name() string {
	return j.name
}
