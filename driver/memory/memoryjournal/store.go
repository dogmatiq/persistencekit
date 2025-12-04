package memoryjournal

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that stores records in
// memory.
type Store[T any] struct {
	journals sync.Map // map[string]*journalState
}

// BinaryStore is an implementation of [journal.BinaryStore] that stores records
// in memory.
type BinaryStore = Store[[]byte]

// Open returns the journal with the given name.
func (s *Store[T]) Open(ctx context.Context, name string) (journal.Journal[T], error) {
	st, ok := s.journals.Load(name)

	if !ok {
		st, _ = s.journals.LoadOrStore(
			name,
			&state[T]{},
		)
	}

	return &journ[T]{
		name:  name,
		state: st.(*state[T]),
	}, ctx.Err()
}
