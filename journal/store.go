package journal

import (
	"context"
)

// Store is a collection of journals containing records of type T.
type Store[T any] interface {
	Open(ctx context.Context, name string) (Journal[T], error)
}
