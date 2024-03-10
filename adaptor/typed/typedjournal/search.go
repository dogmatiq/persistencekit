package typedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/journal"
)

// CompareFunc is a function that compares a record to some datum.
//
// If the record is less than the datum, cmp is negative. If the record is
// greater than the datum, cmp is positive. Otherwise, the record is considered
// equal to the datum.
type CompareFunc[Record any] func(
	ctx context.Context,
	pos journal.Position,
	rec Record,
) (cmp int, err error)

// BinarySearch performs a binary search of the journal to find the position of
// the record for which cmp() returns zero.
func BinarySearch[
	Record any,
	Marshaler typedmarshaler.Marshaler[Record],
](
	ctx context.Context,
	j Journal[Record, Marshaler],
	begin, end journal.Position,
	cmp CompareFunc[Record],
) (journal.Position, Record, error) {
	var rec Record
	pos, _, err := journal.BinarySearch(
		ctx,
		j.Journal,
		begin, end,
		func(ctx context.Context, pos journal.Position, data []byte) (int, error) {
			var err error
			rec, err = j.marshaler.Unmarshal(data)
			if err != nil {
				return 0, err
			}
			return cmp(ctx, pos, rec)
		},
	)
	return pos, rec, err
}
