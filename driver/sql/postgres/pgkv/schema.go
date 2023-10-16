package pgkv

import (
	"context"
	"database/sql"
)

// CreateSchema creates the PostgreSQL schema elements required by [Store].
func CreateSchema(
	ctx context.Context,
	db *sql.DB,
) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint:errcheck

	if _, err := db.ExecContext(
		ctx,
		`CREATE SCHEMA IF NOT EXISTS persistencekit`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS persistencekit.kv (
			keyspace TEXT NOT NULL,
			key      BYTEA NOT NULL,
			value    BYTEA NOT NULL,

			PRIMARY KEY (keyspace, key)
		)`,
	); err != nil {
		return err
	}

	return nil
}
