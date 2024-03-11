package typedjournal

import (
	"context"
	"errors"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/journal"
)

// IsFresh returns true if j has never contained any records.
func IsFresh[
	Record any,
	Marshaler typedmarshaler.Marshaler[Record],
](
	ctx context.Context,
	j *Journal[Record, Marshaler],
) (bool, error) {
	_, end, err := j.Bounds(ctx)
	return end == 0, err
}

// IsEmpty returns true if j does not currently contain any records.
func IsEmpty[
	Record any,
	Marshaler typedmarshaler.Marshaler[Record],
](
	ctx context.Context,
	j *Journal[Record, Marshaler],
) (bool, error) {
	begin, end, err := j.Bounds(ctx)
	return begin == end, err
}

// FirstRecord returns the oldest record in a journal.
func FirstRecord[
	Record any,
	Marshaler typedmarshaler.Marshaler[Record],
](
	ctx context.Context,
	j *Journal[Record, Marshaler],
) (journal.Position, Record, bool, error) {
	for {
		begin, end, err := j.Bounds(ctx)
		if begin == end || err != nil {
			return 0, typedmarshaler.Zero[Record](), false, err
		}

		rec, err := j.Get(ctx, begin)

		if !errors.Is(err, journal.ErrNotFound) {
			return begin, rec, true, err
		}

		// We didn't find the record. Assuming the journal is not corrupted,
		// that means that it was truncated after the call to Bounds() but
		// before the call to Get(), so we re-read the bounds and try again.
	}
}

// LastRecord returns the newest record in a journal.
func LastRecord[
	Record any,
	Marshaler typedmarshaler.Marshaler[Record],
](
	ctx context.Context,
	j *Journal[Record, Marshaler],
) (journal.Position, Record, bool, error) {
	for {
		begin, end, err := j.Bounds(ctx)
		if begin == end || err != nil {
			return 0, typedmarshaler.Zero[Record](), false, err
		}

		pos := end - 1
		rec, err := j.Get(ctx, pos)

		if !errors.Is(err, journal.ErrNotFound) {
			return pos, rec, true, err
		}

		// We didn't find the record. Assuming the journal is not corrupted,
		// that means that it was truncated after the call to Bounds() but
		// before the call to Get(), so we re-read the bounds and try again.
	}
}
