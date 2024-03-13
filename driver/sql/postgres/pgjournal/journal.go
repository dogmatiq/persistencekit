package pgjournal

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/persistencekit/journal"
)

// journ is an implementation of [journal.Journal] that persists to a PostgreSQL
// database.
type journ struct {
	Name string
	DB   *sql.DB
}

func (j *journ) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	row := j.DB.QueryRowContext(
		ctx,
		`SELECT
			COALESCE(MIN(position),     0),
			COALESCE(MAX(position) + 1, 0)
		FROM persistencekit.journal
		WHERE name = $1`,
		j.Name,
	)

	if err := row.Scan(&begin, &end); err != nil {
		return 0, 0, fmt.Errorf("cannot query journal bounds: %w", err)
	}

	return begin, end, nil
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	row := j.DB.QueryRowContext(
		ctx,
		`SELECT record
		FROM persistencekit.journal
		WHERE name = $1
		AND position = $2`,
		j.Name,
		pos,
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
	fn journal.RangeFunc,
) error {
	// TODO: "paginate" results across multiple queries to avoid loading
	// everything into memory at once.
	rows, err := j.DB.QueryContext(
		ctx,
		`SELECT position, record
		FROM persistencekit.journal
		WHERE name = $1
		AND position >= $2
		ORDER BY position`,
		j.Name,
		begin,
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
		if err := rows.Scan(&pos, &rec); err != nil {
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

	return nil
}

func (j *journ) Append(ctx context.Context, end journal.Position, rec []byte) error {
	res, err := j.DB.ExecContext(
		ctx,
		`INSERT INTO persistencekit.journal
		(name, position, record) VALUES ($1, $2, $3)
		ON CONFLICT (name, position) DO NOTHING`,
		j.Name,
		end,
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
	if _, err := j.DB.ExecContext(
		ctx,
		`DELETE FROM persistencekit.journal
		WHERE name = $1
		AND position < $2`,
		j.Name,
		end,
	); err != nil {
		return fmt.Errorf("cannot update journal bounds: %w", err)
	}

	return nil
}

func (j *journ) Close() error {
	return nil
}
