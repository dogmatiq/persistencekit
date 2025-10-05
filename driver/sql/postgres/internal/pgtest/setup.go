package pgtest

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Setup creates and returns a new PostgreSQL database connection for use in a
// test. The database is automatically cleaned up when the test ends.
func Setup(t testing.TB) *sql.DB {
	username := "persistencekit"
	password := uuid.NewString()

	container, err := postgres.Run(
		t.Context(),
		"postgres",
		postgres.BasicWaitStrategies(),
		postgres.WithUsername(username),
		postgres.WithPassword(password),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Log(err)
		}
	})

	dsn, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("cannot open test database: %s", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	return db
}
