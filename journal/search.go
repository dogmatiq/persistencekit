package journal

import (
	"context"
)

// CompareFunc is a function that compares a record to some datum.
//
// If the record is less than the datum, cmp is negative. If the record is
// greater than the datum, cmp is positive. Otherwise, the record is considered
// equal to the datum.
type CompareFunc[T any] func(context.Context, Position, T) (cmp int, err error)

// Search performs a binary search of j within the half-open range [begin, end)
// to find the position of the record for which cmp() returns zero.
//
// It returns [ErrNotFound] if there is no such record.
func Search[T any](
	ctx context.Context,
	j Journal[T],
	begin, end Position,
	cmp CompareFunc[T],
) (pos Position, rec T, err error) {
	for begin < end {
		pos := (begin >> 1) + (end >> 1)

		rec, err := j.Get(ctx, pos)
		if err != nil {
			return 0, rec, err
		}

		result, err := cmp(ctx, pos, rec)
		if err != nil {
			return 0, rec, err
		}

		if result > 0 {
			end = pos
		} else if result < 0 {
			begin = pos + 1
		} else {
			return pos, rec, nil
		}
	}

	return 0, rec, ErrNotFound
}

// RangeFromSearchResult invokes fn for each record in the journal, in order,
// beginning with the record for which cmp() returns zero.
//
// It returns [ErrNotFound] if there is no such record.
func RangeFromSearchResult[T any](
	ctx context.Context,
	j Journal[T],
	begin, end Position,
	cmp CompareFunc[T],
	fn RangeFunc[T],
) error {
	pos, rec, err := Search(ctx, j, begin, end, cmp)
	if err != nil {
		return err
	}

	ok, err := fn(ctx, pos, rec)
	if !ok || err != nil {
		return err
	}

	return j.Range(ctx, pos+1, fn)
}
