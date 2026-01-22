package pgjournal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/commonschema"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal/internal/xdb"
	"github.com/dogmatiq/persistencekit/journal"
)

// BinaryStore is an implementation of [journal.BinaryStore] that persists to a
// PostgreSQL database.
type BinaryStore struct {
	// DB is the PostgreSQL database connection.
	DB *sql.DB
}

// Open returns the journal with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	queries := xdb.New(s.DB)

	for {
		id, err := queries.UpsertJournal(ctx, name)
		if err == nil {
			return &journ{s.DB, queries, id, name}, nil
		}

		if !pgerror.Is(err, pgerror.CodeUndefinedTable) {
			return nil, err
		}

		if err := commonschema.Create(ctx, s.DB, xdb.Schema); err != nil {
			return nil, fmt.Errorf("cannot create journal schema: %w", err)
		}
	}
}
