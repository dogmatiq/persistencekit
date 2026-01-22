package commonschema

import (
	"context"
	"database/sql"
	_ "embed"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/pgerror"
)

//go:embed schema.sql
var schema string

// Create creates the PostgreSQL schema elements required by
// all PostgreSQL-based stores.
func Create(
	ctx context.Context,
	db *sql.DB,
	additional ...string,
) error {
	return pgerror.Retry(
		ctx,
		db,
		func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, schema); err != nil {
				return err
			}

			for _, q := range additional {
				if _, err := tx.ExecContext(ctx, q); err != nil {
					return err
				}
			}

			return nil
		},
		// Even though we use IF NOT EXISTS in the DDL, we still need to handle
		// conflicts due to a data race bug in PostgreSQL.
		pgerror.CodeUniqueViolation,
	)
}
