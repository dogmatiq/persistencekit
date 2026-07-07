package pgtest

import (
	"database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Setup creates and returns a new PostgreSQL database connection for use in a
// test. It also returns the DSN used to connect. The database is automatically
// cleaned up when the test ends.
func Setup(t testing.TB) (*sql.DB, string) {
	username := "persistencekit"
	password := uuid.NewString()

	container, err := postgres.Run(
		t.Context(),
		"postgres",
		postgres.BasicWaitStrategies(),
		postgres.WithUsername(username),
		postgres.WithPassword(password),

		// Allow container reuse, but key it based on the same session ID that
		// testcontainers does for starting the Ryuk reaper process; otherwise
		// the container will be shutdown by the first reaper process that
		// starts.
		testcontainers.WithReuseByName(
			fmt.Sprintf(
				"dogmatiq-persistencekit-postgres-%s",
				testcontainers.SessionID(),
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	mainDSN, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("unable to read PostgreSQL DSN: %s", err)
	}

	dbName := createTestDatabase(t, mainDSN)
	testDSN := replaceDatabaseNameInDSN(t, mainDSN, dbName)

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("unable to open PostgreSQL connection pool: %s", err)
	}

	db.SetMaxOpenConns(100)

	t.Cleanup(func() {
		db.Close()
	})

	return db, testDSN
}

// createTestDatabase creates a new database with a random name on the server
// specified by the given DSN. It returns the name.
func createTestDatabase(t testing.TB, dsn string) (dbName string) {
	t.Helper()

	dbName = "dogma-" + uuidpb.Generate().AsString()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("unable to open PostgreSQL connection pool: %s", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(
		t.Context(),
		`CREATE DATABASE "`+dbName+`"`,
	); err != nil {
		t.Fatalf("unable to create PostgreSQL database: %s", err)
	}

	return dbName
}

// replaceDatabaseNameInDSN replaces the database name in the given PostgreSQL
// connection string with the given name and returns the modified connection
// string.
func replaceDatabaseNameInDSN(
	t testing.TB,
	dsn string,
	dbName string,
) string {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("unable to parse PostgreSQL connection string: %s", err)
	}
	u.Path = "/" + dbName

	return u.String()
}
