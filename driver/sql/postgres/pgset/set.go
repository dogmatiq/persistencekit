package pgset

import (
	"context"
	"database/sql"
	"fmt"
)

type setimpl struct {
	db   *sql.DB
	id   uint64
	name string
}

func (s *setimpl) Name() string {
	return s.name
}

func (s *setimpl) Has(ctx context.Context, k []byte) (bool, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(value) != 0
		FROM persistencekit.set_value
		WHERE set_id = $1
		AND value = $2`,
		s.id,
		k,
	)

	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, fmt.Errorf("cannot scan set value: %w", err)
	}

	return exists, nil
}

func (s *setimpl) Add(ctx context.Context, v []byte) error {
	_, err := s.insert(ctx, v)
	return err
}

func (s *setimpl) TryAdd(ctx context.Context, v []byte) (bool, error) {
	res, err := s.insert(ctx, v)
	if err != nil {
		return false, err
	}
	return checkRowAffected(res)
}

func (s *setimpl) Remove(ctx context.Context, v []byte) error {
	_, err := s.delete(ctx, v)
	return err
}

func (s *setimpl) TryRemove(ctx context.Context, v []byte) (bool, error) {
	res, err := s.delete(ctx, v)
	if err != nil {
		return false, err
	}
	return checkRowAffected(res)
}

func (s *setimpl) Close() error {
	return nil
}

func (s *setimpl) insert(ctx context.Context, v []byte) (sql.Result, error) {
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO persistencekit.set_value AS o (
			set_id,
			value
		) VALUES (
			$1, $2
		) ON CONFLICT (set_id, value) DO NOTHING
		`,
		s.id,
		v,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot insert value into set: %w", err)
	}

	return res, nil
}

func (s *setimpl) delete(ctx context.Context, v []byte) (sql.Result, error) {
	res, err := s.db.ExecContext(
		ctx,
		`DELETE FROM persistencekit.set_value
		WHERE set_id = $1
		AND value = $2`,
		s.id,
		v,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot delete value from set: %w", err)
	}

	return res, nil
}

func checkRowAffected(res sql.Result) (bool, error) {
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot get rows affected: %w", err)
	}

	if rows == 0 {
		return false, nil
	}

	if rows == 1 {
		return true, nil
	}

	return false, fmt.Errorf(
		"unexpected number of rows affected: %d, expected 0 or 1",
		rows,
	)
}
