package pgkv

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/persistencekit/kv"
)

// Store is an implementation of [kv.Store] that persists to a PostgreSQL
// database.
type Store struct {
	DB *sql.DB
}

// Open returns the keyspace with the given name.
func (s *Store) Open(ctx context.Context, name string) (kv.Keyspace, error) {
	return &keyspace{
		Name: name,
		DB:   s.DB,
	}, nil
}
