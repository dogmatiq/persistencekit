package kv

import (
	"context"
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

func (ks *mkeyspace[K, V]) Get(ctx context.Context, k K) (v V, err error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		var zero V
		return zero, err
	}

	valueData, err := ks.BinaryKeyspace.Get(ctx, keyData)
	if err != nil || len(valueData) == 0 {
		var zero V
		return zero, err
	}

	return ks.vm.Unmarshal(valueData)
}

func (ks *mkeyspace[K, V]) Has(ctx context.Context, k K) (bool, error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return false, err
	}
	return ks.BinaryKeyspace.Has(ctx, keyData)
}

func (ks *mkeyspace[K, V]) Set(ctx context.Context, k K, v V) error {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return err
	}

	var valueData []byte
	if !reflect.ValueOf(v).IsZero() {
		valueData, err = ks.vm.Marshal(v)
		if err != nil {
			return err
		}
	}

	return ks.BinaryKeyspace.Set(ctx, keyData, valueData)
}

func (ks *mkeyspace[K, V]) Range(ctx context.Context, fn RangeFunc[K, V]) error {
	return ks.BinaryKeyspace.Range(
		ctx,
		func(ctx context.Context, k, v []byte) (bool, error) {
			key, err := ks.km.Unmarshal(k)
			if err != nil {
				return false, err
			}

			value, err := ks.vm.Unmarshal(v)
			if err != nil {
				return false, err
			}

			return fn(ctx, key, value)
		},
	)
}
