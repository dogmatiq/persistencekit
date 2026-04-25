package pgjournal

import (
	"context"
	"database/sql"
	_ "embed"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
)

//go:embed schema.sql
var schema string

// Provision creates the PostgreSQL schema and tables used by the store if they
// do not already exist.
//
// The store also creates the schema on first use if it does not exist.
// Provision allows infrastructure to be created ahead of time, for example as
// part of a deployment pipeline, so that the application itself does not need
// DDL permissions.
func (s *BinaryStore) Provision(ctx context.Context) error {
	return pgerror.Retry(
		ctx,
		s.DB,
		func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, schema)
			return err
		},
		// Even though we use IF NOT EXISTS in the DDL, we still need to handle
		// conflicts due to a data race bug in PostgreSQL.
		pgerror.CodeUniqueViolation,
	)
}
