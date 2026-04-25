package pgjournal_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgtest"
	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestStore(t *testing.T) {
	db := pgtest.Setup(t)
	journal.RunTests(
		t,
		&BinaryStore{
			DB: db,
		},
	)
}

func BenchmarkStore(b *testing.B) {
	db := pgtest.Setup(b)
	journal.RunBenchmarks(
		b,
		&BinaryStore{
			DB: db,
		},
	)
}
