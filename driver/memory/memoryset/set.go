package memoryset

import (
	"context"
	"errors"
	"maps"
	"sync"

	"github.com/dogmatiq/persistencekit/set"
)

// state is the in-memory state of a set.
type state[C comparable] struct {
	sync.RWMutex
	Values map[C]struct{}
}

// setimpl is an implementation of [kv.BinarySet] that manipulates a
// setimpl's in-memory [state].
type setimpl[T any, C comparable] struct {
	name           string
	state          *state[C]
	marshalValue   func(T) C
	unmarshalValue func(C) T
}

func (s *setimpl[T, C]) Name() string {
	return s.name
}

func (s *setimpl[T, C]) Has(ctx context.Context, v T) (ok bool, err error) {
	if s.state == nil {
		panic("set is closed")
	}

	c := s.marshalValue(v)

	s.state.RLock()
	defer s.state.RUnlock()

	_, ok = s.state.Values[c]
	return ok, ctx.Err()
}

func (s *setimpl[T, C]) Add(ctx context.Context, v T) error {
	_, err := s.TryAdd(ctx, v)
	return err
}

func (s *setimpl[T, C]) TryAdd(ctx context.Context, v T) (bool, error) {
	if s.state == nil {
		panic("set is closed")
	}

	c := s.marshalValue(v)

	s.state.Lock()
	defer s.state.Unlock()

	if s.state.Values == nil {
		s.state.Values = map[C]struct{}{}
	}

	before := len(s.state.Values)
	s.state.Values[c] = struct{}{}
	after := len(s.state.Values)

	return before < after, ctx.Err()
}

func (s *setimpl[T, C]) Remove(ctx context.Context, v T) error {
	_, err := s.TryRemove(ctx, v)
	return err
}

func (s *setimpl[T, C]) TryRemove(ctx context.Context, v T) (bool, error) {
	if s.state == nil {
		panic("set is closed")
	}

	c := s.marshalValue(v)

	s.state.Lock()
	defer s.state.Unlock()

	if s.state.Values == nil {
		return false, ctx.Err()
	}

	before := len(s.state.Values)
	delete(s.state.Values, c)
	after := len(s.state.Values)

	return before > after, ctx.Err()
}

func (s *setimpl[T, C]) Range(ctx context.Context, fn set.RangeFunc[T]) error {
	if s.state == nil {
		panic("set is closed")
	}

	s.state.RLock()
	values := maps.Clone(s.state.Values)
	s.state.RUnlock()

	for c := range values {
		v := s.unmarshalValue(c)
		ok, err := fn(ctx, v)
		if !ok || err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (s *setimpl[T, C]) Close() error {
	if s.state == nil {
		return errors.New("set is already closed")
	}

	s.state = nil

	return nil
}
