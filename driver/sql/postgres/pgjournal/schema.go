package pgjournal

import (
	"context"
	"database/sql"
)

// CreateSchema creates the PostgreSQL schema elements required by
// [JournalStore].
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
		`CREATE TABLE IF NOT EXISTS persistencekit.journal (
			name     TEXT NOT NULL,
			position BIGINT NOT NULL,
			record   BYTEA NOT NULL,

			PRIMARY KEY (name, position)
		)`,
	); err != nil {
		return err
	}

	return nil
}
