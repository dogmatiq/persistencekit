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
	Bounds  journal.Interval
	Records []T
}

// journ is an implementation of [journal.Journal] that manipulates a journal's
// in-memory [state].
type journ[T any] struct {
	name         string
	state        *state[T]
	beforeAppend func(name string, rec T) error
	afterAppend  func(name string, rec T) error
}

func (j *journ[T]) Bounds(ctx context.Context) (bounds journal.Interval, err error) {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	defer j.state.RUnlock()

	return j.state.Bounds, ctx.Err()
}

func (j *journ[T]) Get(ctx context.Context, pos journal.Position) (T, error) {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	defer j.state.RUnlock()

	if !j.state.Bounds.Contains(pos) {
		var zero T
		return zero, journal.ErrNotFound
	}

	index := pos - j.state.Bounds.Begin
	return clone.Clone(j.state.Records[index]), ctx.Err()
}

func (j *journ[T]) Range(
	ctx context.Context,
	pos journal.Position,
	fn journal.RangeFunc[T],
) error {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	bounds := j.state.Bounds
	records := j.state.Records
	j.state.RUnlock()

	if !bounds.Contains(pos) {
		return journal.ErrNotFound
	}

	start := pos - bounds.Begin
	bounds.Begin = pos
	records = records[start:]

	for index, pos := range bounds.Positions() {
		rec := clone.Clone(records[index])
		ok, err := fn(ctx, pos, rec)
		if !ok || err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (j *journ[T]) Append(ctx context.Context, pos journal.Position, rec T) error {
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
	case pos < j.state.Bounds.End:
		return journal.ErrConflict
	case pos == j.state.Bounds.End:
		j.state.Records = append(j.state.Records, rec)
		j.state.Bounds.End++
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

func (j *journ[T]) Truncate(ctx context.Context, pos journal.Position) error {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.Lock()
	defer j.state.Unlock()

	if pos > j.state.Bounds.End {
		panic("position out of range, this causes undefined behavior in a real journal implementation")
	}

	if pos > j.state.Bounds.Begin {
		j.state.Records = j.state.Records[pos-j.state.Bounds.Begin:]
		j.state.Bounds.Begin = pos
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
