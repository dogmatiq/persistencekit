package kv

import "context"

// A RangeFunc is a function used to range over the key/value pairs in a
// [Keyspace].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
//
// k is the key, v is the value, and r is the current revision.
type RangeFunc[K, V any] func(ctx context.Context, k K, v V, r uint64) (ok bool, err error)

// A Keyspace is an isolated collection of key/value pairs.
type Keyspace[K, V any] interface {
	// Name returns the name of the keyspace.
	Name() string

	// Get returns the value associated with k.
	//
	// r is a monotonically increasing revision number that changes each time
	// the value associated with k is modified.
	//
	// If the key does not exist v is the zero-value of V and r is zero.
	Get(ctx context.Context, k K) (v V, r uint64, err error)

	// Has returns true if k is present in the keyspace.
	Has(ctx context.Context, k K) (ok bool, err error)

	// Set associates a value with k.
	//
	// If v is the zero-value of V (or equivalent), the key is deleted.
	//
	// r is the current revision number for k. If k is not present in the
	// keyspace, its current revision is zero. If r does not match the current
	// revision, a [ConflictError] occurs.
	//
	// On success, the new revision number is always r + 1.
	Set(ctx context.Context, k K, v V, r uint64) error

	// Range invokes fn for each key in the keyspace in an undefined order.
	Range(ctx context.Context, fn RangeFunc[K, V]) error

	// Close closes the keyspace.
	Close() error
}
