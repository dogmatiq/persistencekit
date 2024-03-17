package journal

import (
	"context"
)

// ScanFunc is a predicate function that produces a value of type V from a
// record of type T.
//
// If the record cannot be used to produce a value of type V, ok is false.
type ScanFunc[T, V any] func(ctx context.Context, pos Position, rec T) (v V, ok bool, err error)

// Scan finds a value of type V within the journal by scanning all records
// beginning with the record at the given position.
//
// It returns [ErrNotFound] if the value is not found.
//
// This function is useful when the value being searched is not ordered, or when
// there are a small number of records to scan. If the records are structured
// in such a way that it's possible to know if the value appears before or after
// a specific record, use [Search] instead.
func Scan[T, V any](
	ctx context.Context,
	j Journal[T],
	begin Position,
	scan ScanFunc[T, V],
) (V, error) {
	var (
		v  V
		ok bool
	)

	if err := j.Range(
		ctx,
		begin,
		func(ctx context.Context, pos Position, rec T) (bool, error) {
			var err error
			v, ok, err = scan(ctx, pos, rec)
			return !ok, err
		},
	); err != nil {
		return v, err
	}
	if !ok {
		return v, ErrNotFound
	}

	return v, nil
}

// ScanFromSearchResult finds a value within the journal by scanning all records
// beginning with the record for which a binary search of the half-open range
// [begin, end) using cmp as the comparator returns true. See [Scan] and
// [Search].
//
// It returns [ErrNotFound] if the value is not found.
func ScanFromSearchResult[T, V any](
	ctx context.Context,
	j Journal[T],
	begin, end Position,
	cmp CompareFunc[T],
	scan ScanFunc[T, V],
) (V, error) {
	pos, rec, err := Search(ctx, j, begin, end, cmp)
	if err != nil {
		var zero V
		return zero, err
	}

	v, ok, err := scan(ctx, pos, rec)
	if ok || err != nil {
		return v, err
	}

	return Scan(ctx, j, pos+1, scan)
}
