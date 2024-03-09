package typedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
)

// StoreOf is a journal store for journals that store records of type R.
type StoreOf[R any, M Marshaler[R]] struct {
	journal.Store
	Marshaler M
}

// Open returns the journal with the given name.
func (s StoreOf[R, M]) Open(ctx context.Context, name string) (Journal[R, M], error) {
	j, err := s.Store.Open(ctx, name)
	return Journal[R, M]{j, s.Marshaler}, err
}
