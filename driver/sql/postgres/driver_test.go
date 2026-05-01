package postgres_test

import (
	"net/url"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgtest"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgset"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
)

func TestNew(t *testing.T) {
	db, dsn := pgtest.Setup(t)

	cfg, err := postgres.ParseURL(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}

	d, err := postgres.New(cfg)
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

func TestNewFromDB(t *testing.T) {
	db, _ := pgtest.Setup(t)

	d := postgres.NewFromDB(db)
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

	cfg, err := postgres.ParseURL(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}

	d, err := cfg.NewDriver(t.Context())
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

func TestFromURL(t *testing.T) {
	t.Run("it returns a working driver", func(t *testing.T) {
		db, dsn := pgtest.Setup(t)

		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}

		cfg, err := postgres.FromURL(t.Context(), parsed)
		if err != nil {
			t.Fatal(err)
		}

		d, err := cfg.NewDriver(t.Context())
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
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct {
			Name string
			URL  *url.URL
		}{
			{"wrong scheme", &url.URL{Scheme: "other", Host: "localhost", Path: "/db"}},
			{"invalid config", &url.URL{Scheme: "postgres", Host: "localhost:notaport", Path: "/db"}},
		}
		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := postgres.FromURL(t.Context(), tc.URL)
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		}
	})
}
