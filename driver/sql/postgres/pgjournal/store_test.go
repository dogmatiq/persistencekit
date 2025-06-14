package pgjournal_test

import (
	"database/sql"
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/sqltest"
)

func TestStore(t *testing.T) {
	db := setup(t)
	journal.RunTests(
		t,
		&BinaryStore{
			DB: db,
		},
	)
}

func BenchmarkStore(b *testing.B) {
	db := setup(b)
	journal.RunBenchmarks(
		b,
		&BinaryStore{
			DB: db,
		},
	)
}

func setup(t testing.TB) *sql.DB {
	database, err := sqltest.NewDatabase(
		t.Context(),
		sqltest.PGXDriver,
		sqltest.PostgreSQL,
	)
	if err != nil {
		t.Fatalf("cannot create test database: %s", err)
	}

	db, err := database.Open()
	if err != nil {
		t.Fatalf("cannot open test database: %s", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("cannot close database: %s", err)
		}

		if err := database.Close(); err != nil {
			t.Errorf("cannot close test database: %s", err)
		}
	})

	return db
}
