package memoryset

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/set"
)

// Store is an in-memory implementation of [set.Store].
type Store[T comparable] struct {
	state sync.Map // map[string]*state[T]
}

// Open returns the set with the given name.
func (s *Store[T]) Open(ctx context.Context, name string) (set.Set[T], error) {
	st, ok := s.state.Load(name)

	if !ok {
		st, _ = s.state.LoadOrStore(
			name,
			&state[T]{},
		)
	}

	return &setimpl[T, T]{
		name:           name,
		state:          st.(*state[T]),
		marshalValue:   identity[T],
		unmarshalValue: identity[T],
	}, ctx.Err()
}

func identity[K any](k K) K {
	return k
}

// BinaryStore is an implementation of [set.BinaryStore] that stores
// sets in memory.
type BinaryStore struct {
	state sync.Map // map[string]*state[string]
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (set.BinarySet, error) {
	st, ok := s.state.Load(name)

	if !ok {
		st, _ = s.state.LoadOrStore(
			name,
			&state[string]{},
		)
	}

	return &setimpl[[]byte, string]{
		name:           name,
		state:          st.(*state[string]),
		marshalValue:   func(k []byte) string { return string(k) },
		unmarshalValue: func(k string) []byte { return []byte(k) },
	}, ctx.Err()
}
