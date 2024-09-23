package journal

import (
	"context"
)

// Position is the index of a record within a [Journal]. The first record is always
// at position 0.
type Position uint64

// A RangeFunc is a function used to range over the records in a [Journal].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type RangeFunc[T any] func(context.Context, Position, T) (ok bool, err error)

// A Journal is an append-only log containing records of type T.
type Journal[T any] interface {
	// Name returns the name of the journal.
	Name() string

	// Bounds returns the half-open interval [begin, end) describing the
	// positions of the first and last records in the journal.
	Bounds(ctx context.Context) (Interval, error)

	// Get returns the record at the given position.
	//
	// It returns a [RecordNotFoundError] if there is no record at the given
	// position.
	Get(ctx context.Context, pos Position) (rec T, err error)

	// Range invokes fn for each record in the journal, in order, starting with
	// the record at the given position.
	//
	// It returns a [RecordNotFoundError] if there is no record at the given
	// position. if there is no record at the given position.
	Range(ctx context.Context, pos Position, fn RangeFunc[T]) error

	// Append adds a record to the journal as the given position.
	//
	// The record is stored at the given position and the end of the journal
	// becomes pos + 1.
	//
	// pos must be the end of the journal, as returned by [Bounds]. If pos < end
	// then [ErrConflict] is returned, indicating that there is already a record
	// at the given position. The behavior is undefined if pos > end.
	Append(ctx context.Context, pos Position, rec T) error

	// Truncate removes journal records in the half-open interval [begin, pos),
	// such that pos becomes the new beginning of the journal.
	//
	// If it returns an error the truncation may have been partially applied.
	//
	// The behavior is undefined if pos > end.
	Truncate(ctx context.Context, pos Position) error

	// Close closes the journal.
	Close() error
}
