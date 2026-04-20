package kv

import "context"

// Revision is an opaque token that identifies the "version" or "generation" of
// a key/value pair within a [Keyspace]. A non-existent key always has an empty
// revision.
type Revision string

// A RangeFunc is a function used to range over the key/value pairs in a
// [Keyspace].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
//
// k is the key, v is the value, and r is the current revision.
type RangeFunc[K, V any] func(ctx context.Context, k K, v V, r Revision) (ok bool, err error)

// A Keyspace is an isolated collection of key/value pairs.
type Keyspace[K, V any] interface {
	// Name returns the name of the keyspace.
	Name() string

	// Get returns the value associated with k.
	//
	// r is an opaque token that changes each time the value associated with k
	// is modified.
	//
	// If the key does not exist v is the zero-value of V and r is empty.
	Get(ctx context.Context, k K) (v V, r Revision, err error)

	// Has returns true if k is present in the keyspace.
	Has(ctx context.Context, k K) (ok bool, err error)

	// Set associates a value with k.
	//
	// If v is the zero-value of V (or equivalent), the key is deleted.
	//
	// r is the current revision of k. If k is not present in the keyspace, its
	// current revision is the empty string. If r does not match the current
	// revision, a [ConflictError] occurs.
	//
	// On success, the new revision is returned.
	Set(ctx context.Context, k K, v V, r Revision) (Revision, error)

	// SetUnconditional associates a value with k, regardless of its current
	// revision.
	//
	// It is equivalent to calling Set with the current revision number, but
	// offers no optimistic concurrency control.
	//
	// If v is the zero-value of V (or equivalent), the key is deleted.
	SetUnconditional(ctx context.Context, k K, v V) error

	// Range invokes fn for each key in the keyspace in an undefined order.
	Range(ctx context.Context, fn RangeFunc[K, V]) error

	// Close closes the keyspace.
	Close() error
}
