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

func (ks *mkeyspace[K, V]) Get(ctx context.Context, k K) (v V, t []byte, err error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		var zero V
		return zero, nil, err
	}

	valueData, t, err := ks.BinaryKeyspace.Get(ctx, keyData)
	if err != nil || len(valueData) == 0 {
		var zero V
		return zero, t, err
	}

	v, err = ks.vm.Unmarshal(valueData)
	return v, t, err
}

func (ks *mkeyspace[K, V]) Has(ctx context.Context, k K) (bool, error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return false, err
	}
	return ks.BinaryKeyspace.Has(ctx, keyData)
}

func (ks *mkeyspace[K, V]) Set(ctx context.Context, k K, v V, t []byte) ([]byte, error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return nil, err
	}

	var valueData []byte
	if !reflect.ValueOf(v).IsZero() {
		valueData, err = ks.vm.Marshal(v)
		if err != nil {
			return nil, err
		}
	}

	t, err = ks.BinaryKeyspace.Set(ctx, keyData, valueData, t)
	if err != nil {
		var conflict ConflictError[K]

		// Re-package the conflict error so that it uses a key of type K,
		// instead of []byte.
		if errors.As(err, &conflict) {
			return nil, ConflictError[K]{
				Keyspace: conflict.Keyspace,
				Key:      k,
				Token:    conflict.Token,
			}
		}

		return nil, err
	}

	return t, nil
}

func (ks *mkeyspace[K, V]) Range(ctx context.Context, fn RangeFunc[K, V]) error {
	return ks.BinaryKeyspace.Range(
		ctx,
		func(ctx context.Context, keyData, valueData, t []byte) (bool, error) {
			k, err := ks.km.Unmarshal(keyData)
			if err != nil {
				return false, err
			}

			v, err := ks.vm.Unmarshal(valueData)
			if err != nil {
				return false, err
			}

			return fn(ctx, k, v, t)
		},
	)
}
