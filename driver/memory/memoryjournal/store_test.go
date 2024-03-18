package memoryjournal_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestStore(t *testing.T) {
	journal.RunTests(
		t,
		&BinaryStore{},
	)
}

func BenchmarkStore(b *testing.B) {
	journal.RunBenchmarks(
		b,
		&BinaryStore{},
	)
}
