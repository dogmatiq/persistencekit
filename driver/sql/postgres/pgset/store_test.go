package pgset_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgtest"
	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgset"
	"github.com/dogmatiq/persistencekit/set"
)

func TestStore(t *testing.T) {
	db := pgtest.Setup(t)
	set.RunTests(
		t,
		&BinaryStore{
			DB: db,
		},
	)
}

func BenchmarkStore(b *testing.B) {
	db := pgtest.Setup(b)
	set.RunBenchmarks(
		b,
		&BinaryStore{
			DB: db,
		},
	)
}
