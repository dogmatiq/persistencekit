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

	err = row.Scan(&begin, &end)
	return begin, end, err
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
	err := row.Scan(&rec)
	if err == sql.ErrNoRows {
		return nil, journal.ErrNotFound
	}

	return rec, err
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
		return err
	}
	defer rows.Close()

	expectPos := begin

	for rows.Next() {
		var (
			pos journal.Position
			rec []byte
		)
		if err := rows.Scan(&pos, &rec); err != nil {
			return err
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

	return rows.Err()
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
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n != 1 {
		return journal.ErrConflict
	}

	return nil
}

func (j *journ) Truncate(ctx context.Context, end journal.Position) error {
	_, err := j.DB.ExecContext(
		ctx,
		`DELETE FROM persistencekit.journal
		WHERE name = $1
		AND position < $2`,
		j.Name,
		end,
	)

	return err
}

func (j *journ) Close() error {
	return nil
}