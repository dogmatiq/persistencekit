package typedkv

import (
	"context"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/kv"
)

// Store is a collection of keyspaces that maps keys of type K to values of type
// V.
type Store[
	Key, Value any,
	KeyMarshaler typedmarshaler.Marshaler[Key],
	ValueMarshaler typedmarshaler.Marshaler[Value],
] struct {
	kv.Store
	KeyMarshaler   KeyMarshaler
	ValueMarshaler ValueMarshaler
}

// Open returns the journal with the given name.
func (s Store[K, V, KM, VM]) Open(ctx context.Context, name string) (*Keyspace[K, V, KM, VM], error) {
	ks, err := s.Store.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &Keyspace[K, V, KM, VM]{
		ks,
		s.KeyMarshaler,
		s.ValueMarshaler,
	}, nil
}
