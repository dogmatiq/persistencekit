package pgkv

import (
	"context"
	"database/sql"
	_ "embed"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
)

//go:embed schema.sql
var schema string

// createSchema creates the PostgreSQL schema elements required by [BinaryStore].
func createSchema(
	ctx context.Context,
	db *sql.DB,
) error {
	return pgerror.Retry(
		ctx,
		db,
		func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, schema)
			return err
		},
		// Even though we use IF NOT EXISTS in the DDL, we still need to handle
		// conflicts due to a data race bug in PostgreSQL.
		pgerror.CodeUniqueViolation,
	)
}
