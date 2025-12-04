package memoryset

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/set"
)

// Store is an in-memory implementation of [set.Store].
type Store[T comparable] struct {
	// BeforeOpen, if non-nil, is called before a set is opened.
	BeforeOpen func(name string) error

	// BeforeAdd, if non-nil, is called before a value is added to the set.
	BeforeAdd func(set string, v T) error

	// AfterAdd, if non-nil, is called after a value is added to the set.
	AfterAdd func(set string, v T) error

	// BeforeRemove, if non-nil, is called before a value is removed from the
	// set.
	BeforeRemove func(set string, v T) error

	// AfterRemove, if non-nil, is called after a value is removed from the set.
	AfterRemove func(set string, v T) error

	state sync.Map // map[string]*state[T]
}

// Open returns the set with the given name.
func (s *Store[T]) Open(ctx context.Context, name string) (set.Set[T], error) {
	if s.BeforeOpen != nil {
		if err := s.BeforeOpen(name); err != nil {
			return nil, err
		}
	}

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
		beforeAdd:      s.BeforeAdd,
		afterAdd:       s.AfterAdd,
		beforeRemove:   s.BeforeRemove,
		afterRemove:    s.AfterRemove,
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
	// BeforeOpen, if non-nil, is called before a set is opened.
	BeforeOpen func(name string) error

	// BeforeAdd, if non-nil, is called before a value is added to the set.
	BeforeAdd func(set string, v []byte) error

	// AfterAdd, if non-nil, is called after a value is added to the set.
	AfterAdd func(set string, v []byte) error

	// BeforeRemove, if non-nil, is called before a value is removed from the
	// set.
	BeforeRemove func(set string, v []byte) error

	// AfterRemove, if non-nil, is called after a value is removed from the set.
	AfterRemove func(set string, v []byte) error

	state sync.Map // map[string]*state[string]
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (set.BinarySet, error) {
	if s.BeforeOpen != nil {
		if err := s.BeforeOpen(name); err != nil {
			return nil, err
		}
	}

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
		beforeAdd:      s.BeforeAdd,
		afterAdd:       s.AfterAdd,
		beforeRemove:   s.BeforeRemove,
		afterRemove:    s.AfterRemove,
		marshalValue:   func(k []byte) string { return string(k) },
		unmarshalValue: func(k string) []byte { return []byte(k) },
	}, ctx.Err()
}
