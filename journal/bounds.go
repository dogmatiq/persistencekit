package journal

import (
	"context"
	"errors"
)

// IsFresh returns true if j has never contained any records.
func IsFresh[T any](ctx context.Context, j Journal[T]) (bool, error) {
	_, end, err := j.Bounds(ctx)
	return end == 0, err
}

// IsEmpty returns true if j does not currently contain any records.
func IsEmpty[T any](ctx context.Context, j Journal[T]) (bool, error) {
	begin, end, err := j.Bounds(ctx)
	return begin == end, err
}

// FirstRecord returns the oldest record in a journal.
func FirstRecord[T any](ctx context.Context, j Journal[T]) (Position, T, bool, error) {
	for {
		begin, end, err := j.Bounds(ctx)
		if begin == end || err != nil {
			var zero T
			return 0, zero, false, err
		}

		rec, err := j.Get(ctx, begin)

		if !errors.Is(err, ErrNotFound) {
			return begin, rec, true, err
		}

		// We didn't find the record. Assuming the journal is not corrupted,
		// that means that it was truncated after the call to Bounds() but
		// before the call to Get(), so we re-read the bounds and try again.
	}
}

// LastRecord returns the newest record in a journal.
func LastRecord[T any](ctx context.Context, j Journal[T]) (Position, T, bool, error) {
	for {
		begin, end, err := j.Bounds(ctx)
		if begin == end || err != nil {
			var zero T
			return 0, zero, false, err
		}

		pos := end - 1
		rec, err := j.Get(ctx, pos)

		if !errors.Is(err, ErrNotFound) {
			return pos, rec, true, err
		}

		// We didn't find the record. Assuming the journal is not corrupted,
		// that means that it was truncated after the call to Bounds() but
		// before the call to Get(), so we re-read the bounds and try again.
	}
}
