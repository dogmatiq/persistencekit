package set

import (
	"context"

	"github.com/dogmatiq/persistencekit/marshaler"
)

// NewMarshalingStore returns a new [Store] that marshals/unmarshals values of
// type T to/from an underlying [BinaryStore].
func NewMarshalingStore[T any](
	s BinaryStore,
	m marshaler.Marshaler[T],
) Store[T] {
	return &mstore[T]{s, m}
}

// mstore is an implementation of [Store] that marshals/unmarshals values
// pairs to/from an underlying [BinaryStore].
type mstore[T any] struct {
	BinaryStore
	m marshaler.Marshaler[T]
}

func (s *mstore[T]) Open(ctx context.Context, name string) (Set[T], error) {
	set, err := s.BinaryStore.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &mset[T]{set, s.m}, nil
}

// mset is an implementation of [Set] that marshals/unmarshals
// values to/from an underlying [BinarySet].
type mset[T any] struct {
	BinarySet
	m marshaler.Marshaler[T]
}

func (s *mset[T]) Has(ctx context.Context, v T) (bool, error) {
	data, err := s.m.Marshal(v)
	if err != nil {
		return false, err
	}
	return s.BinarySet.Has(ctx, data)
}

func (s *mset[T]) Add(ctx context.Context, v T) error {
	data, err := s.m.Marshal(v)
	if err != nil {
		return err
	}

	return s.BinarySet.Add(ctx, data)
}

func (s *mset[T]) TryAdd(ctx context.Context, v T) (bool, error) {
	data, err := s.m.Marshal(v)
	if err != nil {
		return false, err
	}
	return s.BinarySet.TryAdd(ctx, data)
}

func (s *mset[T]) Remove(ctx context.Context, v T) error {
	data, err := s.m.Marshal(v)
	if err != nil {
		return err
	}

	return s.BinarySet.Remove(ctx, data)
}

func (s *mset[T]) TryRemove(ctx context.Context, v T) (bool, error) {
	data, err := s.m.Marshal(v)
	if err != nil {
		return false, err
	}
	return s.BinarySet.TryRemove(ctx, data)
}

func (s *mset[T]) Range(ctx context.Context, fn RangeFunc[T]) error {
	return s.BinarySet.Range(
		ctx,
		func(ctx context.Context, v []byte) (bool, error) {
			value, err := s.m.Unmarshal(v)
			if err != nil {
				return false, err
			}

			return fn(ctx, value)
		},
	)
}
