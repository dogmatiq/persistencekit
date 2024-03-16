package memoryjournal_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestStore(t *testing.T) {
	journal.RunTests(
		t,
		func(t *testing.T) journal.Store {
			return &Store{}
		},
	)
}

func BenchmarkStore(b *testing.B) {
	journal.RunBenchmarks(
		b,
		func(b *testing.B) journal.Store {
			return &Store{}
		},
	)
}
