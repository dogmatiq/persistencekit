package typedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is a collection of keyspaces that store values of type R.
type Store[R any, M Marshaler[R]] struct {
	journal.Store
	Marshaler M
}

// Open returns the journal with the given name.
func (s Store[R, M]) Open(ctx context.Context, name string) (Journal[R, M], error) {
	j, err := s.Store.Open(ctx, name)
	return Journal[R, M]{j, s.Marshaler}, err
}
