package kv

import "context"

// A RangeFunc is a function used to range over the key/value pairs in a
// [Keyspace].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type RangeFunc[K, V any] func(ctx context.Context, k K, v V) (ok bool, err error)

// A Keyspace is an isolated collection of key/value pairs.
type Keyspace[K, V any] interface {
	// Get returns the value associated with k.
	//
	// If the key does not exist v is the zero-value of V.
	Get(ctx context.Context, k K) (v V, err error)

	// Has returns true if k is present in the keyspace.
	Has(ctx context.Context, k K) (ok bool, err error)

	// Set associates a value with k.
	//
	// If v is the zero-value of V (or equivalent), the key is deleted.
	Set(ctx context.Context, k K, v V) error

	// Range invokes fn for each key in the keyspace in an undefined order.
	Range(ctx context.Context, fn RangeFunc[K, V]) error

	// Close closes the keyspace.
	Close() error
}
