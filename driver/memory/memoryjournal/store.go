package memoryjournal

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that stores records in memory.
type Store struct {
	// BeforeOpen, if non-nil, is called before a journal is opened.
	BeforeOpen func(name string) error

	// BeforeAppend, if non-nil, is called before a record is appended.
	BeforeAppend func(name string, rec []byte) error

	// AfterAppend, if non-nil, is called after a record is appended.
	AfterAppend func(name string, rec []byte) error

	journals sync.Map // map[string]*journalState
}

// Open returns the journal with the given name.
func (s *Store) Open(ctx context.Context, name string) (journal.Journal, error) {
	if s.BeforeOpen != nil {
		if err := s.BeforeOpen(name); err != nil {
			return nil, err
		}
	}

	st, ok := s.journals.Load(name)

	if !ok {
		st, _ = s.journals.LoadOrStore(
			name,
			&state{},
		)
	}

	return &journ{
		name:         name,
		state:        st.(*state),
		beforeAppend: s.BeforeAppend,
		afterAppend:  s.AfterAppend,
	}, ctx.Err()
}
