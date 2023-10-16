package pgkv

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/persistencekit/kv"
)

// KeyValueStore is an implementation of [kv.Store] that stores keyspaces in a
// PostgreSQL database.
type KeyValueStore struct {
	DB *sql.DB
}

// Open returns the keyspace with the given name.
func (s *KeyValueStore) Open(_ context.Context, name string) (kv.Keyspace, error) {
	// TODO: consider creating a separate table partition for each keyspace
	return &keyspace{
		Name: name,
		DB:   s.DB,
	}, nil
}
