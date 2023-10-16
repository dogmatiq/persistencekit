package pgkv_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/sqltest"
)

func TestStore(t *testing.T) {
	ctx := context.Background()

	database, err := sqltest.NewDatabase(ctx, sqltest.PGXDriver, sqltest.PostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	db, err := database.Open()
	if err != nil {
		t.Fatal(err)
	}

	if err := CreateSchema(ctx, db); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	})

	kv.RunTests(
		t,
		func(t *testing.T) kv.Store {
			return &Store{
				DB: db,
			}
		},
	)
}
