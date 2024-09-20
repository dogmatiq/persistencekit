package pgjournal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/bigint"
	"github.com/dogmatiq/persistencekit/journal"
)

// journ is an implementation of [journal.BinaryJournal] that persists to a PostgreSQL
// database.
type journ struct {
	db *sql.DB
	id uint64
}

func (j *journ) Bounds(ctx context.Context) (bounds journal.Interval, err error) {
	row := j.db.QueryRowContext(
		ctx,
		`SELECT
			j.encoded_begin,
			j.encoded_end
		FROM persistencekit.journal AS j
		WHERE j.id = $1`,
		j.id,
	)

	if err := row.Scan(
		bigint.ConvertUnsigned(&bounds.Begin),
		bigint.ConvertUnsigned(&bounds.End),
	); err != nil {
		return journal.Interval{}, fmt.Errorf("cannot query journal bounds: %w", err)
	}

	return bounds, nil
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	row := j.db.QueryRowContext(
		ctx,
		`SELECT record
		FROM persistencekit.journal_record
		WHERE journal_id = $1
		AND encoded_position = $2`,
		j.id,
		bigint.ConvertUnsigned(&pos),
	)

	var rec []byte
	if err := row.Scan(&rec); err != nil {
		if err == sql.ErrNoRows {
			return nil, journal.RecordNotFoundError{Position: pos}
		}
		return nil, fmt.Errorf("cannot scan journal record: %w", err)
	}

	return rec, nil
}

func (j *journ) Range(
	ctx context.Context,
	pos journal.Position,
	fn journal.BinaryRangeFunc,
) error {
	// TODO: "paginate" results across multiple queries to avoid loading
	// everything into memory at once.
	rows, err := j.db.QueryContext(
		ctx,
		`SELECT encoded_position, record
		FROM persistencekit.journal_record
		WHERE journal_id = $1
		AND encoded_position >= $2
		ORDER BY encoded_position`,
		j.id,
		bigint.ConvertUnsigned(&pos),
	)
	if err != nil {
		return fmt.Errorf("cannot query journal records: %w", err)
	}
	defer rows.Close()

	expectPos := pos

	for rows.Next() {
		var (
			pos journal.Position
			rec []byte
		)
		if err := rows.Scan(
			bigint.ConvertUnsigned(&pos),
			&rec,
		); err != nil {
			return fmt.Errorf("cannot scan journal record: %w", err)
		}

		if pos != expectPos {
			return journal.RecordNotFoundError{Position: expectPos}
		}

		expectPos++

		ok, err := fn(ctx, pos, rec)
		if !ok || err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot range over journal records: %w", err)
	}

	if expectPos == pos {
		return journal.RecordNotFoundError{Position: pos}
	}

	return nil
}

func (j *journ) Append(ctx context.Context, pos journal.Position, rec []byte) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cannot begin append transaction: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(
		ctx,
		`UPDATE persistencekit.journal
		SET encoded_end = encoded_end + 1
		WHERE id = $1
		AND encoded_end = $2`,
		j.id,
		bigint.ConvertUnsigned(&pos),
	)
	if err != nil {
		return fmt.Errorf("cannot update journal bounds: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot determine affected rows: %w", err)
	}

	if n == 0 {
		return journal.ConflictError{Position: pos}
	}

	res, err = tx.ExecContext(
		ctx,
		`INSERT INTO persistencekit.journal_record
		(journal_id, encoded_position, record) VALUES ($1, $2, $3)`,
		j.id,
		bigint.ConvertUnsigned(&pos),
		rec,
	)
	if err != nil {
		return fmt.Errorf("cannot insert journal record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit append transaction: %w", err)
	}

	return nil
}

func (j *journ) Truncate(ctx context.Context, pos journal.Position) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cannot begin truncate transaction: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(
		ctx,
		`UPDATE persistencekit.journal
		SET encoded_begin = $2
		WHERE id = $1
		AND encoded_begin < $2`,
		j.id,
		bigint.ConvertUnsigned(&pos),
	)
	if err != nil {
		return fmt.Errorf("cannot update journal bounds: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot determine affected rows: %w", err)
	}
	if n == 0 {
		return nil
	}

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM persistencekit.journal_record
		WHERE journal_id = $1
		AND encoded_position < $2`,
		j.id,
		bigint.ConvertUnsigned(&pos),
	); err != nil {
		return fmt.Errorf("cannot truncate journal records: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit truncate transaction: %w", err)
	}

	return nil
}

func (j *journ) Close() error {
	return nil
}
