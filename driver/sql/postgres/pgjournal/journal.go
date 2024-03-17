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

func (j *journ) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	row := j.db.QueryRowContext(
		ctx,
		`SELECT
			COALESCE(MIN(encoded_position),     -1::BIGINT << 63),
			COALESCE(MAX(encoded_position) + 1, -1::BIGINT << 63)
		FROM persistencekit.journal_record
		WHERE journal_id = $1`,
		j.id,
	)

	if err := row.Scan(
		bigint.ConvertUnsigned(&begin),
		bigint.ConvertUnsigned(&end),
	); err != nil {
		return 0, 0, fmt.Errorf("cannot query journal bounds: %w", err)
	}

	return begin, end, nil
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
			return nil, journal.ErrNotFound
		}
		return nil, fmt.Errorf("cannot scan journal record: %w", err)
	}

	return rec, nil
}

func (j *journ) Range(
	ctx context.Context,
	begin journal.Position,
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
		bigint.ConvertUnsigned(&begin),
	)
	if err != nil {
		return fmt.Errorf("cannot query journal records: %w", err)
	}
	defer rows.Close()

	expectPos := begin

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
			return journal.ErrNotFound
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

	if expectPos == begin {
		return journal.ErrNotFound
	}

	return nil
}

func (j *journ) Append(ctx context.Context, end journal.Position, rec []byte) error {
	res, err := j.db.ExecContext(
		ctx,
		`INSERT INTO persistencekit.journal_record
		(journal_id, encoded_position, record) VALUES ($1, $2, $3)
		ON CONFLICT (journal_id, encoded_position) DO NOTHING`,
		j.id,
		bigint.ConvertUnsigned(&end),
		rec,
	)
	if err != nil {
		return fmt.Errorf("cannot insert journal record: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot determine affected rows: %w", err)
	}

	if n != 1 {
		return journal.ErrConflict
	}

	return nil
}

func (j *journ) Truncate(ctx context.Context, end journal.Position) error {
	if _, err := j.db.ExecContext(
		ctx,
		`DELETE FROM persistencekit.journal_record
		WHERE journal_id = $1
		AND encoded_position < $2`,
		j.id,
		bigint.ConvertUnsigned(&end),
	); err != nil {
		return fmt.Errorf("cannot truncate journal records: %w", err)
	}

	return nil
}

func (j *journ) Close() error {
	return nil
}
