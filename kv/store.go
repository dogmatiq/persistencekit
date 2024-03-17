package kv

import (
	"context"
)

// Store is a collection of keyspaces that map keys of type K to values of type
// V.
type Store[K, V any] interface {
	// Open returns the keyspace with the given name.
	Open(ctx context.Context, name string) (Keyspace[K, V], error)
}
