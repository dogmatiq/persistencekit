package pgjournal

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that persists to a PostgreSQL
// database.
type Store struct {
	// DB is the PostgreSQL database connection.
	DB *sql.DB
}

// Open returns the journal with the given name.
func (s *Store) Open(_ context.Context, name string) (journal.Journal, error) {
	// TODO: consider creating a separate table partition for each journal
	return &journ{
		Name: name,
		DB:   s.DB,
	}, nil
}
