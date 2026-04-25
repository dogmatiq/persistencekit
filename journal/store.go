package journal

import (
	"context"
)

// Store is a collection of journals containing records of type T.
type Store[T any] interface {
	// Open returns the journal with the given name.
	Open(ctx context.Context, name string) (Journal[T], error)

	// Provision creates the infrastructure used by the store if it does not
	// already exist.
	Provision(ctx context.Context) error
}
