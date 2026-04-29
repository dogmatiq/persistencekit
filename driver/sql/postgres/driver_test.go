package postgres_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgtest"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgset"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
)

func TestNew(t *testing.T) {
	db, _ := pgtest.Setup(t)

	d := postgres.New(db)
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		&pgjournal.BinaryStore{DB: db},
		&pgkv.BinaryStore{DB: db},
		&pgset.BinaryStore{DB: db},
	)
}

func TestParseURL(t *testing.T) {
	db, dsn := pgtest.Setup(t)

	open, err := postgres.ParseURL(dsn)
	if err != nil {
		t.Fatal(err)
	}

	d, err := open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		&pgjournal.BinaryStore{DB: db},
		&pgkv.BinaryStore{DB: db},
		&pgset.BinaryStore{DB: db},
	)
}
