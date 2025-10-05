package pgkv_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgtest"
	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/kv"
)

func TestStore(t *testing.T) {
	db := pgtest.Setup(t)
	kv.RunTests(
		t,
		&BinaryStore{
			DB: db,
		},
	)
}

func BenchmarkStore(b *testing.B) {
	db := pgtest.Setup(b)
	kv.RunBenchmarks(
		b,
		&BinaryStore{
			DB: db,
		},
	)
}
