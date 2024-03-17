package journal

import (
	"context"

	"github.com/dogmatiq/persistencekit/marshaler"
)

// NewMarshalingStore returns a new [Store] that marshals/unmarshals records of
// type T to/from an underlying [BinaryStore].
func NewMarshalingStore[T any](
	s BinaryStore,
	m marshaler.Marshaler[T],
) Store[T] {
	return &mstore[T]{s, m}
}

// mstore is an implementation of [Store] that marshals/unmarshals records of
// type T to/from an underlying [BinaryStore].
type mstore[T any] struct {
	BinaryStore
	m marshaler.Marshaler[T]
}

func (s *mstore[T]) Open(ctx context.Context, name string) (Journal[T], error) {
	j, err := s.BinaryStore.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return &mjourn[T]{j, s.m}, nil
}

// A journ is an implementation of [Journal] that marshals/unmarshals records of
// type T to/from an underlying [BinaryJournal].
type mjourn[T any] struct {
	BinaryJournal
	m marshaler.Marshaler[T]
}

func (j *mjourn[T]) Get(ctx context.Context, pos Position) (T, error) {
	data, err := j.BinaryJournal.Get(ctx, pos)
	if err != nil {
		var zero T
		return zero, err
	}

	return j.m.Unmarshal(data)
}

func (j *mjourn[T]) Range(ctx context.Context, pos Position, fn RangeFunc[T]) error {
	return j.BinaryJournal.Range(
		ctx,
		pos,
		func(ctx context.Context, pos Position, data []byte) (bool, error) {
			rec, err := j.m.Unmarshal(data)
			if err != nil {
				return false, err
			}

			return fn(ctx, pos, rec)
		},
	)
}

func (j *mjourn[T]) Append(ctx context.Context, end Position, rec T) error {
	data, err := j.m.Marshal(rec)
	if err != nil {
		return err
	}

	return j.BinaryJournal.Append(ctx, end, data)
}
