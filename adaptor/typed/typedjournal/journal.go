package typedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/journal"
)

// A journ is an implementation of [journal.Journal] that marshals/unmarshals
// records of type T to/from an underlying [journal.BinaryJournal].
type journ[
	T any,
	M typedmarshaler.Marshaler[T],
] struct {
	journal.BinaryJournal
	m M
}

// Get returns the record at the given position.
//
// It returns [journal.ErrNotFound] if there is no record at the given position.
func (j *journ[R, M]) Get(ctx context.Context, pos journal.Position) (R, error) {
	data, err := j.BinaryJournal.Get(ctx, pos)
	if err != nil {
		return typedmarshaler.Zero[R](), err
	}

	return j.m.Unmarshal(data)
}

// Range invokes fn for each record in the journal, in order, starting with
// the record at the given position.
//
// It returns [journal.ErrNotFound] if there is no record at the given position.
func (j *journ[R, M]) Range(ctx context.Context, pos journal.Position, fn journal.RangeFunc[R]) error {
	return j.BinaryJournal.Range(
		ctx,
		pos,
		func(ctx context.Context, pos journal.Position, data []byte) (bool, error) {
			rec, err := j.m.Unmarshal(data)
			if err != nil {
				return false, err
			}

			return fn(ctx, pos, rec)
		},
	)
}

// Append adds a record to the journal.
//
// end must be the next "unused" position in the journal; the first position is
// always 0.
//
// If there is already a record at the given position then [journal.ErrConflict]
// is returned, indicating an optimistic concurrency conflict.
//
// The behavior is undefined if end is greater than the next position.
func (j *journ[R, M]) Append(ctx context.Context, end journal.Position, rec R) error {
	data, err := j.m.Marshal(rec)
	if err != nil {
		return err
	}

	return j.BinaryJournal.Append(ctx, end, data)
}
