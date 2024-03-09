package journal

import (
	"context"
)

// CompareFunc is a function that compares a record to some datum.
//
// If the record is less than the datum, cmp is negative. If the record is
// greater than the datum, cmp is positive. Otherwise, the record is considered
// equal to the datum.
type CompareFunc func(context.Context, Position, []byte) (cmp int, err error)

// BinarySearch performs a binary search of j to find the position of the record
// for which cmp() returns zero.
func BinarySearch(
	ctx context.Context,
	j Journal,
	begin, end Position,
	cmp CompareFunc,
) (pos Position, rec []byte, err error) {
	for begin < end {
		pos := (begin >> 1) + (end >> 1)

		rec, err := j.Get(ctx, pos)
		if err != nil {
			return 0, nil, err
		}

		result, err := cmp(ctx, pos, rec)
		if err != nil {
			return 0, nil, err
		}

		if result > 0 {
			end = pos
		} else if result < 0 {
			begin = pos + 1
		} else {
			return pos, rec, nil
		}
	}

	return 0, nil, ErrNotFound
}
