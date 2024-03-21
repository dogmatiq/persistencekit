package partitionedjournal_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/persistencekit/adaptor/partitioned/partitionedjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestStore(t *testing.T) {
	journal.RunTests(
		t,
		func(t *testing.T) journal.Store {
			store := &memoryjournal.Store{}
			return &Store{
				SelectPartition: func(context.Context, string) (journal.Store, error) {
					return store, nil
				},
			}
		},
	)
}
