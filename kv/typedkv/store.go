package typedkv

import (
	"context"

	"github.com/dogmatiq/persistencekit/kv"
)

// Store is a collection of keyspaces that maps keys of type K to values of type
// V.
type Store[K, V any, KM Marshaler[K], VM Marshaler[V]] struct {
	kv.Store
	KeyMarshaler   KM
	ValueMarshaler VM
}

// Open returns the journal with the given name.
func (s Store[K, V, KM, VM]) Open(ctx context.Context, name string) (Keyspace[K, V, KM, VM], error) {
	ks, err := s.Store.Open(ctx, name)
	return Keyspace[K, V, KM, VM]{ks, s.KeyMarshaler, s.ValueMarshaler}, err
}
