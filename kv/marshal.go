package kv

import (
	"context"
	"errors"
	"reflect"

	"github.com/dogmatiq/persistencekit/marshaler"
)

// NewMarshalingStore returns a new [Store] that marshals/unmarshals key/value
// pairs to/from an underlying [BinaryStore].
func NewMarshalingStore[K, V any](
	s BinaryStore,
	km marshaler.Marshaler[K],
	vm marshaler.Marshaler[V],
) Store[K, V] {
	return &mstore[K, V]{s, km, vm}
}

// mstore is an implementation of [Store] that marshals/unmarshals key/value
// pairs to/from an underlying [BinaryStore].
type mstore[K, V any] struct {
	BinaryStore
	km marshaler.Marshaler[K]
	vm marshaler.Marshaler[V]
}

func (s *mstore[K, V]) Open(ctx context.Context, name string) (Keyspace[K, V], error) {
	ks, err := s.BinaryStore.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &mkeyspace[K, V]{ks, s.km, s.vm}, nil
}

// mkeyspace is an implementation of [Keyspace] that marshals/unmarshals
// key/value pairs to/from an underlying [BinaryKeyspace].
type mkeyspace[K, V any] struct {
	BinaryKeyspace
	km marshaler.Marshaler[K]
	vm marshaler.Marshaler[V]
}

func (ks *mkeyspace[K, V]) Get(ctx context.Context, k K) (v V, r uint64, err error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		var zero V
		return zero, 0, err
	}

	valueData, r, err := ks.BinaryKeyspace.Get(ctx, keyData)
	if err != nil || len(valueData) == 0 {
		var zero V
		return zero, r, err
	}

	v, err = ks.vm.Unmarshal(valueData)
	return v, r, err
}

func (ks *mkeyspace[K, V]) Has(ctx context.Context, k K) (bool, error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return false, err
	}
	return ks.BinaryKeyspace.Has(ctx, keyData)
}

func (ks *mkeyspace[K, V]) Set(ctx context.Context, k K, v V, r uint64) error {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return err
	}

	valueData, err := ks.marshalValue(v)
	if err != nil {
		return err
	}

	if err := ks.BinaryKeyspace.Set(ctx, keyData, valueData, r); err != nil {
		// Re-package conflict errors to use a key of type K, instead of []byte.
		var conflict ConflictError[[]byte]
		if errors.As(err, &conflict) {
			return ConflictError[K]{
				Keyspace: conflict.Keyspace,
				Key:      k,
				Revision: conflict.Revision,
			}
		}

		return err
	}

	return nil
}

func (ks *mkeyspace[K, V]) SetUnconditional(ctx context.Context, k K, v V) error {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return err
	}

	valueData, err := ks.marshalValue(v)
	if err != nil {
		return err
	}

	return ks.BinaryKeyspace.SetUnconditional(ctx, keyData, valueData)
}

func (ks *mkeyspace[K, V]) marshalValue(v V) ([]byte, error) {
	if reflect.ValueOf(v).IsZero() {
		return nil, nil
	}
	return ks.vm.Marshal(v)
}

func (ks *mkeyspace[K, V]) Range(ctx context.Context, fn RangeFunc[K, V]) error {
	return ks.BinaryKeyspace.Range(
		ctx,
		func(ctx context.Context, keyData, valueData []byte, r uint64) (bool, error) {
			k, err := ks.km.Unmarshal(keyData)
			if err != nil {
				return false, err
			}

			v, err := ks.vm.Unmarshal(valueData)
			if err != nil {
				return false, err
			}

			return fn(ctx, k, v, r)
		},
	)
}
