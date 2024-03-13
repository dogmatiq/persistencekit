package pgjournal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that persists to a PostgreSQL
// database.
type Store struct {
	// DB is the PostgreSQL database connection.
	DB *sql.DB
}

// Open returns the journal with the given name.
func (s *Store) Open(ctx context.Context, name string) (journal.Journal, error) {
	row := s.DB.QueryRowContext(
		ctx,
		`INSERT INTO persistencekit.journal (name)
		VALUES ($1)
		ON CONFLICT (name) DO UPDATE
		SET name = EXCLUDED.name
		RETURNING id`,
		name,
	)

	var id uint64
	if err := row.Scan(&id); err != nil {
		return nil, fmt.Errorf("cannot scan journal ID: %w", err)
	}

	return &journ{
		ID: id,
		DB: s.DB,
	}, nil
}
