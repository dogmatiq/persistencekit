package memoryjournal

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that stores records in
// memory.
type Store[T any] struct {
	// BeforeOpen, if non-nil, is called before a journal is opened.
	BeforeOpen func(name string) error

	// BeforeAppend, if non-nil, is called before a record is appended.
	BeforeAppend func(name string, rec T) error

	// AfterAppend, if non-nil, is called after a record is appended.
	AfterAppend func(name string, rec T) error

	journals sync.Map // map[string]*journalState
}

// BinaryStore is an implementation of [journal.BinaryStore] that stores records
// in memory.
type BinaryStore = Store[[]byte]

// Open returns the journal with the given name.
func (s *Store[T]) Open(ctx context.Context, name string) (journal.Journal[T], error) {
	if s.BeforeOpen != nil {
		if err := s.BeforeOpen(name); err != nil {
			return nil, err
		}
	}

	st, ok := s.journals.Load(name)

	if !ok {
		st, _ = s.journals.LoadOrStore(
			name,
			&state[T]{},
		)
	}

	return &journ[T]{
		name:         name,
		state:        st.(*state[T]),
		beforeAppend: s.BeforeAppend,
		afterAppend:  s.AfterAppend,
	}, ctx.Err()
}
