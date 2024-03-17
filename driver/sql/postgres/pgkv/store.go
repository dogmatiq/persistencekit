package pgkv

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
	"github.com/dogmatiq/persistencekit/kv"
)

// BinaryStore is an implementation of [kv.BinaryStore] that persists to a
// PostgreSQL database.
type BinaryStore struct {
	DB *sql.DB
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (kv.BinaryKeyspace, error) {
	id, err := s.getID(ctx, name)
	if err != nil {
		return nil, err
	}
	return &keyspace{s.DB, id}, nil
}

func (s *BinaryStore) getID(ctx context.Context, name string) (uint64, error) {
	for {
		row := s.DB.QueryRowContext(
			ctx,
			`INSERT INTO persistencekit.keyspace (
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
			return 0, fmt.Errorf("cannot scan keyspace ID: %w", err)
		}

		if err := createSchema(ctx, s.DB); err != nil {
			return 0, fmt.Errorf("cannot create keyspace schema: %w", err)
		}
	}
}
