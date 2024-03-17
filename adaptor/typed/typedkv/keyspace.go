package typedkv

import (
	"context"
	"reflect"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/kv"
)

// A keyspace is an isolated collection of key/value pairs of type K/V.
type keyspace[
	K, V any,
	KM typedmarshaler.Marshaler[K],
	VM typedmarshaler.Marshaler[V],
] struct {
	kv.BinaryKeyspace
	km KM
	vm VM
}

func (ks *keyspace[K, V, KM, VM]) Get(ctx context.Context, k K) (v V, err error) {
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

func (ks *keyspace[K, V, KM, VM]) Has(ctx context.Context, k K) (bool, error) {
	keyData, err := ks.km.Marshal(k)
	if err != nil {
		return false, err
	}
	return ks.BinaryKeyspace.Has(ctx, keyData)
}

func (ks *keyspace[K, V, KM, VM]) Set(ctx context.Context, k K, v V) error {
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

func (ks *keyspace[K, V, KM, VM]) Range(ctx context.Context, fn kv.RangeFunc[K, V]) error {
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
