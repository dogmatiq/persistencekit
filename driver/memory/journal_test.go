package memory_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestJournalStore(t *testing.T) {
	journal.RunTests(
		t,
		func(t *testing.T) journal.Store {
			return &JournalStore{}
		},
	)
}
