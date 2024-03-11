package journal

import (
	"context"
	"errors"
)

// IsFresh returns true if j has never contained any records.
func IsFresh(ctx context.Context, j Journal) (bool, error) {
	_, end, err := j.Bounds(ctx)
	return end == 0, err
}

// IsEmpty returns true if j does not currently contain any records.
func IsEmpty(ctx context.Context, j Journal) (bool, error) {
	begin, end, err := j.Bounds(ctx)
	return begin == end, err
}

// FirstRecord returns the oldest record in a journal.
func FirstRecord(ctx context.Context, j Journal) (Position, []byte, bool, error) {
	for {
		begin, end, err := j.Bounds(ctx)
		if begin == end || err != nil {
			return 0, nil, false, err
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
func LastRecord(ctx context.Context, j Journal) (Position, []byte, bool, error) {
	for {
		begin, end, err := j.Bounds(ctx)
		if begin == end || err != nil {
			return 0, nil, false, err
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
