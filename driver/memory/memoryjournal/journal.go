package memoryjournal

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/dogmatiq/persistencekit/journal"
)

// state is the in-memory state of a journal.
type state struct {
	sync.RWMutex
	Begin, End journal.Position
	Records    [][]byte
}

// journ is an implementation of [journal.Journal] that manipulates a journal's
// in-memory [state].
type journ struct {
	name             string
	state            *state
	beforeAppendHook func(name string, rec []byte) error
	afterAppendHook  func(name string, rec []byte) error
}

func (j *journ) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	defer j.state.RUnlock()

	return j.state.Begin, j.state.End, ctx.Err()
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	if j.state == nil {
		panic("journal is closed")
	}

	j.state.RLock()
	defer j.state.RUnlock()

	if pos < j.state.Begin || pos >= j.state.End {
		return nil, journal.ErrNotFound
	}

	return slices.Clone(j.state.Records[pos-j.state.Begin]), ctx.Err()
}

func (j *journ) Range(
	ctx context.Context,
	begin journal.Position,
	fn journal.RangeFunc,
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
	for i, rec := range records[start:] {
		v := start + journal.Position(i)
		ok, err := fn(ctx, v, slices.Clone(rec))
		if !ok || err != nil {
			return err
		}
		begin++
	}

	return ctx.Err()
}

func (j *journ) Append(ctx context.Context, end journal.Position, rec []byte) error {
	if j.state == nil {
		panic("journal is closed")
	}

	rec = slices.Clone(rec)

	j.state.Lock()
	defer j.state.Unlock()

	if j.beforeAppendHook != nil {
		if err := j.beforeAppendHook(j.name, rec); err != nil {
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

	if j.afterAppendHook != nil {
		if err := j.afterAppendHook(j.name, rec); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (j *journ) Truncate(ctx context.Context, end journal.Position) error {
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

func (j *journ) Close() error {
	if j.state == nil {
		return errors.New("journal is already closed")
	}

	j.state = nil

	return nil
}
