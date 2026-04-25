package pgjournal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
	"github.com/dogmatiq/persistencekit/journal"
)

// BinaryStore is an implementation of [journal.BinaryStore] that persists to a
// PostgreSQL database.
type BinaryStore struct {
	DB *sql.DB
}

// NewBinaryStore returns a new [journal.BinaryStore] that persists to the given
// PostgreSQL database.
func NewBinaryStore(db *sql.DB) *BinaryStore {
	return &BinaryStore{DB: db}
}

// Open returns the journal with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	id, err := s.getID(ctx, name)
	if err != nil {
		return nil, err
	}
	return &journ{s.DB, id, name}, nil
}

func (s *BinaryStore) getID(ctx context.Context, name string) (uint64, error) {
	for {
		row := s.DB.QueryRowContext(
			ctx,
			`INSERT INTO persistencekit.journal (
				name
			) VALUES (
				$1
			) ON CONFLICT (name) DO UPDATE SET
				name = EXCLUDED.name
			RETURNING id`,
			name,
		)

		var id uint64
		err := row.Scan(&id)

		if err == nil {
			return id, nil
		}

		if !pgerror.Is(err, pgerror.CodeUndefinedTable) {
			return 0, fmt.Errorf("cannot scan journal ID: %w", err)
		}

		if err := s.Provision(ctx); err != nil {
			return 0, fmt.Errorf("cannot create journal schema: %w", err)
		}
	}
}
