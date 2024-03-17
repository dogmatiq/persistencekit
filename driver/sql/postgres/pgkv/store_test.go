package pgkv_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/sqltest"
)

func TestStore(t *testing.T) {
	db := setup(t)
	kv.RunTests(
		t,
		func(t *testing.T) kv.BinaryStore {
			return &BinaryStore{
				DB: db,
			}
		},
	)
}

func BenchmarkStore(b *testing.B) {
	db := setup(b)
	kv.RunBenchmarks(
		b,
		func(b *testing.B) kv.BinaryStore {
			return &BinaryStore{
				DB: db,
			}
		},
	)
}

func setup(t testing.TB) *sql.DB {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	database, err := sqltest.NewDatabase(ctx, sqltest.PGXDriver, sqltest.PostgreSQL)
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
