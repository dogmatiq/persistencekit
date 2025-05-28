package set

import "context"

// Set is a unique set of values of type T.
type Set[T any] interface {
	// Name returns the name of the set.
	Name() string

	// Has returns true if the set contains the given value.
	Has(ctx context.Context, v T) (bool, error)

	// Add adds the given value to the set.
	//
	// It is a no-op if v is already present in the set.
	Add(ctx context.Context, v T) error

	// TryAdd adds the given value to the set, it returns true if v was added,
	// or false if it was already present.
	//
	// Add() may be more performant when knowledge of whether v is already
	// present is not required.
	TryAdd(ctx context.Context, v T) (bool, error)

	// Remove removes the given value from the set.
	//
	// It is a no-op if v is not present in the set.
	Remove(ctx context.Context, v T) error

	// TryRemove removes the given value from the set, it returns true if v was
	// removed, or false if it was not present in the set.
	//
	// Remove() may be more performant when knowledge of whether v was
	// present is not required.
	TryRemove(ctx context.Context, v T) (bool, error)

	// Close closes the set.
	Close() error
}
