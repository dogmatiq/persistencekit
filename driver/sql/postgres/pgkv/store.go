package pgkv

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/commonschema"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv/internal/xdb"
	"github.com/dogmatiq/persistencekit/kv"
)

// BinaryStore is an implementation of [kv.BinaryStore] that persists to a
// PostgreSQL database.
type BinaryStore struct {
	DB *sql.DB
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (kv.BinaryKeyspace, error) {
	queries := xdb.New(s.DB)

	for {
		id, err := queries.UpsertKeyspace(ctx, name)

		if err == nil {
			return &keyspace{s.DB, queries, id, name}, nil
		}

		if !pgerror.Is(err, pgerror.CodeUndefinedTable) {
			return nil, err
		}

		if err := commonschema.Create(ctx, s.DB, xdb.Schema); err != nil {
			return nil, fmt.Errorf("cannot create keyspace schema: %w", err)
		}
	}
}
