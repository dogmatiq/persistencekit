package set

import "context"

// A RangeFunc is a function used to range over the members of a [Set].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type RangeFunc[T any] func(ctx context.Context, v T) (ok bool, err error)

// Set is a unique set of values of type T.
type Set[T any] interface {
	// Name returns the name of the set.
	Name() string

	// Has returns true if v is a member of the set.
	Has(ctx context.Context, v T) (bool, error)

	// Add ensures v is a member of the set.
	Add(ctx context.Context, v T) error

	// TryAdd ensures v is a member of the set. It returns true if v was added,
	// or false if it was already a member.
	//
	// Add() may be more performant when knowledge of v's prior membership is
	// not required.
	TryAdd(ctx context.Context, v T) (bool, error)

	// Remove ensures v is not a member of the set.
	Remove(ctx context.Context, v T) error

	// TryRemove ensures v is not a member of the set. It returns true if v was
	// removed, or false if it was not a member.
	//
	// Remove() may be more performant when knowledge of v's prior membership is
	// not required.
	TryRemove(ctx context.Context, v T) (bool, error)

	// Range invokes fn for each member of the set in an undefined order.
	Range(ctx context.Context, fn RangeFunc[T]) error

	// Close closes the set.
	Close() error
}
