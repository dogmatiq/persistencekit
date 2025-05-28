package set

import (
	"context"
)

// Store is a collection of sets that track membership of values of type T.
type Store[T any] interface {
	// Open returns the set with the given name.
	Open(ctx context.Context, name string) (Set[T], error)
}
