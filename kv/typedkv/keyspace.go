package typedkv

import (
	"context"

	"github.com/dogmatiq/persistencekit/kv"
)

// A RangeFunc is a function used to range over the key/value pairs in a
// [Keyspace].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type RangeFunc[K, V any] func(context.Context, K, V) (ok bool, err error)

// A Keyspace is an isolated collection of key/value pairs of type K/V.
type Keyspace[K, V any, KM Marshaler[K], VM Marshaler[V]] struct {
	kv.Keyspace
	keyMarshaler   KM
	valueMarshaler VM
}

// Get returns the value associated with k.
//
// If the key does not exist v is the zero-value and ok is false.
func (ks Keyspace[K, V, KM, VM]) Get(ctx context.Context, k K) (v V, ok bool, err error) {
	keyData, err := ks.keyMarshaler.Marshal(k)
	if err != nil {
		var zero V
		return zero, false, err
	}

	valueData, err := ks.Keyspace.Get(ctx, keyData)
	if err != nil || len(valueData) == 0 {
		var zero V
		return zero, false, err
	}

	v, err = ks.valueMarshaler.Unmarshal(valueData)
	return v, true, err
}

// Has returns true if k is present in the keyspace.
func (ks Keyspace[K, V, KM, VM]) Has(ctx context.Context, k K) (bool, error) {
	keyData, err := ks.keyMarshaler.Marshal(k)
	if err != nil {
		return false, err
	}
	return ks.Keyspace.Has(ctx, keyData)
}

// Set associates a value with k.
//
// If v is marshaled to an empty byte-slice, the key is deleted.
func (ks Keyspace[K, V, KM, VM]) Set(ctx context.Context, k K, v V) error {
	keyData, err := ks.keyMarshaler.Marshal(k)
	if err != nil {
		return err
	}

	valueData, err := ks.valueMarshaler.Marshal(v)
	if err != nil {
		return err
	}

	return ks.Keyspace.Set(ctx, keyData, valueData)
}

// Delete removes the key k from the keyspace.
func (ks Keyspace[K, V, KM, VM]) Delete(ctx context.Context, k K) error {
	keyData, err := ks.keyMarshaler.Marshal(k)
	if err != nil {
		return err
	}
	return ks.Keyspace.Set(ctx, keyData, nil)
}

// Range invokes fn for each key in the keyspace in an undefined order.
func (ks Keyspace[K, V, KM, VM]) Range(ctx context.Context, fn RangeFunc[K, V]) error {
	return ks.Keyspace.Range(
		ctx,
		func(ctx context.Context, k, v []byte) (bool, error) {
			key, err := ks.keyMarshaler.Unmarshal(k)
			if err != nil {
				return false, err
			}

			value, err := ks.valueMarshaler.Unmarshal(v)
			if err != nil {
				return false, err
			}

			return fn(ctx, key, value)
		},
	)
}
