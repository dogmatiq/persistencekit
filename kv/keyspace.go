package kv

import "context"

// A RangeFunc is a function used to range over the key/value pairs in a
// [Keyspace].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
//
// k is the key, v is the value, and t is the concurrency token associated
// with the key/value pair.
type RangeFunc[K, V any] func(ctx context.Context, k K, v V, t []byte) (ok bool, err error)

// A Keyspace is an isolated collection of key/value pairs.
type Keyspace[K, V any] interface {
	// Name returns the name of the keyspace.
	Name() string

	// Get returns the value associated with k.
	//
	// t is a opaque concurrency token representing the current value of k. It
	// is required when setting the key to a new value.
	//
	// If the key does not exist v is the zero-value of V and t is empty.
	Get(ctx context.Context, k K) (v V, t []byte, err error)

	// Has returns true if k is present in the keyspace.
	Has(ctx context.Context, k K) (ok bool, err error)

	// Set associates a value with k.
	//
	// If v is the zero-value of V (or equivalent), the key is deleted.
	//
	// t is an concurrency token that must match the current token for k. If k
	// is not present in the keyspace, it's token is an empty byte slice. If the
	// token does not match, a [ConflictError] occurs.
	//
	// It returns k's new concurrency token.
	Set(ctx context.Context, k K, v V, t []byte) ([]byte, error)

	// Range invokes fn for each key in the keyspace in an undefined order.
	Range(ctx context.Context, fn RangeFunc[K, V]) error

	// Close closes the keyspace.
	Close() error
}
