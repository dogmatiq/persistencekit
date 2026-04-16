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

// Search performs a binary search of j within the interval i to find the
// position of the record for which cmp() returns zero.
//
// It returns a [ValueNotFoundError] if there is no such record.
func Search[T any](
	ctx context.Context,
	j Journal[T],
	i Interval,
	cmp CompareFunc[T],
) (pos Position, rec T, err error) {
	for !i.IsEmpty() {
		pos := (i.Begin >> 1) + (i.End >> 1)

		rec, err := j.Get(ctx, pos)
		if err != nil {
			return 0, rec, err
		}

		result, err := cmp(ctx, pos, rec)
		if err != nil {
			return 0, rec, err
		}

		if result > 0 {
			i.End = pos
		} else if result < 0 {
			i.Begin = pos + 1
		} else {
			return pos, rec, nil
		}
	}

	return 0, rec, ValueNotFoundError{}
}

// AdaptiveProbeFunc is a function that determines the next probe position for
// an [AdaptiveSearch].
//
// i is the current search bracket — the half-open interval of journal positions
// still under consideration.
//
// pos and rec are the position and record of the most recent probe.
//
// If found is true, the record at pos is the target and next is ignored.
//
// When found is false, next must be within i and must differ from pos. If
// either condition is violated, AdaptiveSearch returns a [ValueNotFoundError].
type AdaptiveProbeFunc[T any] func(
	ctx context.Context,
	i Interval,
	pos Position,
	rec T,
) (next Position, found bool, err error)

// AdaptiveSearch searches j within the interval i for a target record, using fn
// to determine which position to probe at each iteration.
//
// Unlike [Search], which always probes the midpoint of the remaining interval,
// AdaptiveSearch allows the caller to choose the next probe position based on
// the content of the previously probed record. This makes it suitable for
// implementing interpolation search and similar algorithms that use record
// content to estimate where the target is likely to be.
//
// probe is the position of the first record to fetch. The caller is expected to
// compute this using any out-of-band knowledge available, such as a previously
// fetched record from the same journal.
//
// It returns a [ValueNotFoundError] if the target record is not found.
func AdaptiveSearch[T any](
	ctx context.Context,
	j Journal[T],
	i Interval,
	probe Position,
	fn AdaptiveProbeFunc[T],
) (Position, T, error) {
	var zero T

	if !i.Contains(probe) {
		return 0, zero, ValueNotFoundError{}
	}

	for !i.IsEmpty() {
		rec, err := j.Get(ctx, probe)
		if err != nil {
			return 0, zero, err
		}

		next, found, err := fn(ctx, i, probe, rec)
		if err != nil {
			return 0, zero, err
		}

		if found {
			return probe, rec, nil
		}

		if next == probe || !i.Contains(next) {
			return 0, zero, ValueNotFoundError{}
		}

		if next > probe {
			i.Begin = probe + 1
		} else {
			i.End = probe
		}

		probe = next
	}

	return 0, zero, ValueNotFoundError{}
}

// RangeFromSearchResult invokes fn for each record in the journal, in order,
// beginning with the record within the interval i for which cmp() returns
// zero.
//
// It returns a [ValueNotFoundError] if there is no such record.
func RangeFromSearchResult[T any](
	ctx context.Context,
	j Journal[T],
	i Interval,
	cmp CompareFunc[T],
	fn RangeFunc[T],
) error {
	pos, rec, err := Search(ctx, j, i, cmp)
	if err != nil {
		return err
	}

	ok, err := fn(ctx, pos, rec)
	if !ok || err != nil {
		return err
	}

	return j.Range(ctx, pos+1, fn)
}
