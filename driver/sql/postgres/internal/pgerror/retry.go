package pgerror

import (
	"context"
	"database/sql"
	"fmt"
)

// Retry executes fn within a transaction, retrying it if the error is one of
// the given codes.
func Retry(
	ctx context.Context,
	db *sql.DB,
	fn func(*sql.Tx) error,
	codes ...string,
) error {
	attempt := 0
	for {
		attempt++
		err := try(ctx, db, fn, attempt)
		if !Is(err, codes...) {
			return err
		}
	}
}

func try(
	ctx context.Context,
	db *sql.DB,
	fn func(*sql.Tx) error,
	attempt int,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cannot start transaction (attempt #%d): %w", attempt, err)
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return fmt.Errorf("cannot perform transaction (attempt #%d): %w", attempt, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction (attempt #%d): %w", attempt, err)
	}

	return nil
}
