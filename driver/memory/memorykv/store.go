package memorykv

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/kv"
)

// Store is an in-memory implementation of [kv.Store].
type Store[K comparable, V any] struct {
	// BeforeSet, if non-nil, is called before a value is set.
	BeforeSet func(ks string, k K, v V) error

	// AfterSet, if non-nil, is called after a value is set.
	AfterSet func(ks string, k K, v V) error

	keyspaces sync.Map // map[string]*state[K, V]
}

// Open returns the keyspace with the given name.
func (s *Store[K, V]) Open(ctx context.Context, name string) (kv.Keyspace[K, V], error) {
	st, ok := s.keyspaces.Load(name)

	if !ok {
		st, _ = s.keyspaces.LoadOrStore(
			name,
			&state[K, V]{},
		)
	}

	return &keyspace[K, V, K]{
		state:        st.(*state[K, V]),
		beforeSet:    s.BeforeSet,
		afterSet:     s.AfterSet,
		marshalKey:   identity[K],
		unmarshalKey: identity[K],
	}, ctx.Err()
}

func identity[K any](k K) K {
	return k
}

// BinaryStore is an implementation of [journal.BinaryStore] that stores records
// in memory.
type BinaryStore struct {
	// BeforeSet, if non-nil, is called before a value is set.
	BeforeSet func(ks string, k, v []byte) error

	// AfterSet, if non-nil, is called after a value is set.
	AfterSet func(ks string, k, v []byte) error

	keyspaces sync.Map // map[string]*state[string, []byte]
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (kv.BinaryKeyspace, error) {
	st, ok := s.keyspaces.Load(name)

	if !ok {
		st, _ = s.keyspaces.LoadOrStore(
			name,
			&state[string, []byte]{},
		)
	}

	return &keyspace[[]byte, []byte, string]{
		state:        st.(*state[string, []byte]),
		beforeSet:    s.BeforeSet,
		afterSet:     s.AfterSet,
		marshalKey:   func(k []byte) string { return string(k) },
		unmarshalKey: func(k string) []byte { return []byte(k) },
	}, ctx.Err()
}
