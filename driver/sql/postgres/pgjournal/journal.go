package pgjournal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/commonschema"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal/internal/xdb"
	"github.com/dogmatiq/persistencekit/journal"
)

// journ is an implementation of [journal.BinaryJournal] that persists to a PostgreSQL
// database.
type journ struct {
	db      *sql.DB
	queries *xdb.Queries
	id      int64
	name    string
}

func (j *journ) Name() string {
	return j.name
}

func (j *journ) Bounds(ctx context.Context) (bounds journal.Interval, err error) {
	row, err := j.queries.SelectBounds(ctx, j.id)
	if err != nil {
		return journal.Interval{}, fmt.Errorf("cannot query journal bounds: %w", err)
	}

	return journal.Interval{
		Begin: journal.Position(row.Begin),
		End:   journal.Position(row.End),
	}, nil
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	rec, err := j.queries.SelectRecord(ctx, xdb.SelectRecordParams{
		JournalID: j.id,
		Position:  commonschema.Uint64(pos),
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, journal.RecordNotFoundError{
				Journal:  j.Name(),
				Position: pos,
			}
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
	params := xdb.SelectRecordsParams{
		JournalID: j.id,
		Position:  commonschema.Uint64(pos),
	}

	for {
		rows, err := j.queries.SelectRecords(ctx, params)
		if err != nil {
			return fmt.Errorf("cannot query journal records: %w", err)
		}

		if len(rows) == 0 {
			if journal.Position(params.Position) == pos {
				return journal.RecordNotFoundError{
					Journal:  j.Name(),
					Position: pos,
				}
			}

			return nil
		}

		for _, row := range rows {
			if row.Position != params.Position {
				return journal.RecordNotFoundError{
					Journal:  j.Name(),
					Position: journal.Position(params.Position),
				}
			}

			ok, err := fn(
				ctx,
				journal.Position(params.Position),
				row.Record,
			)
			if !ok || err != nil {
				return err
			}

			params.Position++
		}
	}
}

func (j *journ) Append(ctx context.Context, pos journal.Position, rec []byte) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cannot begin append transaction: %w", err)
	}
	defer tx.Rollback()

	queries := j.queries.WithTx(tx)

	n, err := queries.IncrementEnd(ctx, xdb.IncrementEndParams{
		JournalID: j.id,
		End:       commonschema.Uint64(pos),
	})
	if err != nil {
		return fmt.Errorf("cannot update journal bounds: %w", err)
	}

	if n == 0 {
		return journal.ConflictError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	if err := queries.InsertRecord(ctx, xdb.InsertRecordParams{
		JournalID: j.id,
		Position:  commonschema.Uint64(pos),
		Record:    rec,
	}); err != nil {
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

	queries := j.queries.WithTx(tx)

	count, err := queries.UpdateBegin(ctx, xdb.UpdateBeginParams{
		JournalID: j.id,
		Begin:     commonschema.Uint64(pos),
	})
	if err != nil {
		return fmt.Errorf("cannot update journal bounds: %w", err)
	}

	if count == 0 {
		return nil
	}

	if err := queries.DeleteRecords(ctx, xdb.DeleteRecordsParams{
		JournalID: j.id,
		End:       commonschema.Uint64(pos),
	}); err != nil {
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
