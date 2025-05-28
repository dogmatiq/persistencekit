package memoryset

import (
	"context"
	"errors"
	"sync"
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
	afterAdd       func(set string, v T) error
	beforeAdd      func(set string, v T) error
	afterRemove    func(set string, v T) error
	beforeRemove   func(set string, v T) error
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

	if s.beforeAdd != nil {
		if err := s.beforeAdd(s.name, v); err != nil {
			return false, err
		}
	}

	if s.state.Values == nil {
		s.state.Values = map[C]struct{}{}
	}

	before := len(s.state.Values)
	s.state.Values[c] = struct{}{}
	after := len(s.state.Values)

	if s.afterAdd != nil {
		if err := s.afterAdd(s.name, v); err != nil {
			return false, err
		}
	}

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

	if s.beforeRemove != nil {
		if err := s.beforeRemove(s.name, v); err != nil {
			return false, err
		}
	}

	if s.state.Values == nil {
		return false, ctx.Err()
	}

	before := len(s.state.Values)
	delete(s.state.Values, c)
	after := len(s.state.Values)

	if s.afterRemove != nil {
		if err := s.afterRemove(s.name, v); err != nil {
			return false, err
		}
	}

	return before > after, ctx.Err()
}

func (s *setimpl[T, C]) Close() error {
	if s.state == nil {
		return errors.New("set is already closed")
	}

	s.state = nil

	return nil
}
