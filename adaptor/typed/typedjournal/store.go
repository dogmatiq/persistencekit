package typedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/adaptor/typed/typedmarshaler"
	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that marshals/unmarshals
// records of type T to/from an underlying [journal.BinaryStore].
type Store[T any, M typedmarshaler.Marshaler[T]] struct {
	journal.BinaryStore
	Marshaler M
}

// Open returns the journal with the given name.
func (s Store[R, M]) Open(ctx context.Context, name string) (journal.Journal[R], error) {
	j, err := s.BinaryStore.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &journ[R, M]{j, s.Marshaler}, nil
}
