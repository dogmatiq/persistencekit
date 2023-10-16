package test_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/test"
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
