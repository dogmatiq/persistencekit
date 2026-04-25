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

// NewBinaryStore returns a new [kv.BinaryStore] that persists to the given
// PostgreSQL database.
func NewBinaryStore(db *sql.DB) *BinaryStore {
	return &BinaryStore{DB: db}
}

// Provision creates the PostgreSQL schema and tables used by the store if they
// do not already exist.
//
// The store also creates the schema on first use if it does not exist.
// Provision allows infrastructure to be created ahead of time, for example as
// part of a deployment pipeline, so that the application itself does not need
// DDL permissions.
func (s *BinaryStore) Provision(ctx context.Context) error {
	return createSchema(ctx, s.DB)
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (kv.BinaryKeyspace, error) {
	id, err := s.getID(ctx, name)
	if err != nil {
		return nil, err
	}
	return &keyspace{s.DB, id, name}, nil
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
