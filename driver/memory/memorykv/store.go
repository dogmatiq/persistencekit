package memorykv

import (
	"context"
	"sync"

	"github.com/dogmatiq/persistencekit/kv"
)

// Store is an in-memory implementation of [kv.Store].
type Store struct {
	// BeforeSet, if non-nil, is called before a value is set.
	BeforeSet func(ks string, k, v []byte) error

	// AfterSet, if non-nil, is called after a value is set.
	AfterSet func(ks string, k, v []byte) error

	keyspaces sync.Map // map[string]*keyspaceState
}

// Open returns the keyspace with the given name.
func (s *Store) Open(ctx context.Context, name string) (kv.Keyspace, error) {
	st, ok := s.keyspaces.Load(name)

	if !ok {
		st, _ = s.keyspaces.LoadOrStore(
			name,
			&state{},
		)
	}

	return &keyspace{
		state:     st.(*state),
		beforeSet: s.BeforeSet,
		afterSet:  s.AfterSet,
	}, ctx.Err()
}
