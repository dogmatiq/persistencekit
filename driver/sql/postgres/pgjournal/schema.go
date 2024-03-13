package pgjournal

import (
	"context"
	"database/sql"
	_ "embed"
)

//go:embed schema.sql
var schema string

// CreateSchema creates the PostgreSQL schema elements required by [Store].
func CreateSchema(
	ctx context.Context,
	db *sql.DB,
) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return err
	}

	return tx.Commit()
}
