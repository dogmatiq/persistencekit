package typedkv

import (
	"context"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/kv"
)

// Store is a collection of keyspaces that maps keys of type K to values of type
// V.
type Store[
	K, V any,
	KM typedmarshaler.Marshaler[K],
	VM typedmarshaler.Marshaler[V],
] struct {
	kv.BinaryStore
	KeyMarshaler   KM
	ValueMarshaler VM
}

// Open returns the journal with the given name.
func (s Store[K, V, KM, VM]) Open(ctx context.Context, name string) (kv.Keyspace[K, V], error) {
	ks, err := s.BinaryStore.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &keyspace[K, V, KM, VM]{
		ks,
		s.KeyMarshaler,
		s.ValueMarshaler,
	}, nil
}
