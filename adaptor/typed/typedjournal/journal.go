package typedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/journal"
)

// A Journal is an append-only log of records of type R.
type Journal[
	Record any,
	Marshaler typedmarshaler.Marshaler[Record],
] struct {
	journal.BinaryJournal
	marshaler Marshaler
}

// Get returns the record at the given position.
//
// It returns [journal.ErrNotFound] if there is no record at the given position.
func (j *Journal[R, M]) Get(ctx context.Context, pos journal.Position) (R, error) {
	data, err := j.BinaryJournal.Get(ctx, pos)
	if err != nil {
		return typedmarshaler.Zero[R](), err
	}

	return j.marshaler.Unmarshal(data)
}

// Range invokes fn for each record in the journal, in order, starting with
// the record at the given position.
//
// It returns [journal.ErrNotFound] if there is no record at the given position.
func (j *Journal[R, M]) Range(ctx context.Context, pos journal.Position, fn journal.RangeFunc[R]) error {
	return j.BinaryJournal.Range(
		ctx,
		pos,
		func(ctx context.Context, pos journal.Position, data []byte) (bool, error) {
			rec, err := j.marshaler.Unmarshal(data)
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
func (j *Journal[R, M]) Append(ctx context.Context, end journal.Position, rec R) error {
	data, err := j.marshaler.Marshal(rec)
	if err != nil {
		return err
	}

	return j.BinaryJournal.Append(ctx, end, data)
}
