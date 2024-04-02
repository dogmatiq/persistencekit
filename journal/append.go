package journal

import (
	"context"
	"errors"
)

// AppendWithConflictResolution appends a record to j using fn to resolve
// optimistic concurrency conflicts. It returns the new journal end position.
//
// If a conflict occurs fn is called and the append is retried with the offset
// it returns. If fn returns an error the append is not retried the error is
// returned.
func AppendWithConflictResolution[T any](
	ctx context.Context,
	j Journal[T],
	end Position,
	rec T,
	fn func(context.Context, Position) (Position, error),
) (Position, error) {
	for {
		err := j.Append(ctx, end, rec)
		if !errors.Is(err, ErrConflict) {
			return end + 1, err
		}

		end, err = fn(ctx, end)
		if err != nil {
			return 0, err
		}
	}
}
