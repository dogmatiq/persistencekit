package pgjournal_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/journal"
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

	if err := CreateJournalStoreSchema(ctx, db); err != nil {
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

	journal.RunTests(
		t,
		func(t *testing.T) journal.Store {
			return &Store{
				DB: db,
			}
		},
	)
}
