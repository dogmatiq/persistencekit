package memoryjournal

import (
	"context"
	"errors"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/internal/clone"
	"github.com/dogmatiq/persistencekit/journal"
)

// state is the in-memory state of a journal.
type state[T any] struct {
	sync.RWMutex
	Begin, End journal.Position
	Records    []T
}

// journ is an implementation of [journal.Journal] that manipulates a journal's
// in-memory [state].
type journ[T any] struct {
	name         string
	state        *state[T]
	beforeAppend func(name string, rec T) error
	afterAppend  func(name string, rec T) error
}

func (j *journ[T]) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	defer j.state.RUnlock()

	return j.state.Begin, j.state.End, ctx.Err()
}

func (j *journ[T]) Get(ctx context.Context, pos journal.Position) (T, error) {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	defer j.state.RUnlock()

	if pos < j.state.Begin || pos >= j.state.End {
		var zero T
		return zero, journal.ErrNotFound
	}

	return clone.Clone(j.state.Records[pos-j.state.Begin]), ctx.Err()
}

func (j *journ[T]) Range(
	ctx context.Context,
	begin journal.Position,
	fn journal.RangeFunc[T],
) error {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	first := j.state.Begin
	records := j.state.Records
	j.state.RUnlock()

	if first > begin {
		return journal.ErrNotFound
	}

	start := begin - first

	if start >= journal.Position(len(records)) {
		return journal.ErrNotFound
	}

	for i, rec := range records[start:] {
		pos := begin + journal.Position(i)
		rec = clone.Clone(rec)

		ok, err := fn(ctx, pos, rec)
		if !ok || err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (j *journ[T]) Append(ctx context.Context, end journal.Position, rec T) error {
	if j.state == nil {
		panic("journal is closed")
	}

	rec = clone.Clone(rec)

	j.state.Lock()
	defer j.state.Unlock()

	if j.beforeAppend != nil {
		if err := j.beforeAppend(j.name, rec); err != nil {
			return err
		}
	}

	switch {
	case end < j.state.End:
		return journal.ErrConflict
	case end == j.state.End:
		j.state.Records = append(j.state.Records, rec)
		j.state.End++
	default:
		panic("position out of range, this causes undefined behavior in a 'real' journal implementation")
	}

	if j.afterAppend != nil {
		if err := j.afterAppend(j.name, rec); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (j *journ[T]) Truncate(ctx context.Context, end journal.Position) error {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.Lock()
	defer j.state.Unlock()

	if end > j.state.End {
		panic("position out of range, this causes undefined behavior in a real journal implementation")
	}

	if end > j.state.Begin {
		j.state.Records = j.state.Records[end-j.state.Begin:]
		j.state.Begin = end
	}

	return ctx.Err()
}

func (j *journ[T]) Close() error {
	if j.state == nil {
		return errors.New("journal is already closed")
	}

	j.state = nil

	return nil
}
