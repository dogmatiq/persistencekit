package pgkv

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/kv"
)

type keyspace struct {
	db   *sql.DB
	id   uint64
	name string
}

func (ks *keyspace) Name() string {
	return ks.name
}

func (ks *keyspace) Get(ctx context.Context, k []byte) ([]byte, error) {
	row := ks.db.QueryRowContext(
		ctx,
		`SELECT value
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1
		AND key = $2`,
		ks.id,
		k,
	)

	var value []byte
	if err := row.Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot scan keyspace pair: %w", err)
	}

	return value, nil
}

func (ks *keyspace) Has(ctx context.Context, k []byte) (bool, error) {
	row := ks.db.QueryRowContext(
		ctx,
		`SELECT COUNT(key) != 0
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1
		AND key = $2`,
		ks.id,
		k,
	)

	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, fmt.Errorf("cannot scan keyspace pair: %w", err)
	}

	return exists, nil
}

func (ks *keyspace) Set(ctx context.Context, k, v []byte) error {
	if len(v) == 0 {
		if _, err := ks.db.ExecContext(
			ctx,
			`DELETE FROM persistencekit.keyspace_pair
			WHERE keyspace_id = $1
			AND key = $2`,
			ks.id,
			k,
		); err != nil {
			return fmt.Errorf("cannot delete keyspace pair: %w", err)
		}
		return nil
	}

	if _, err := ks.db.ExecContext(
		ctx,
		`INSERT INTO persistencekit.keyspace_pair AS o (
			keyspace_id,
			key,
			value
		) VALUES (
			$1, $2, $3
		) ON CONFLICT (keyspace_id, key) DO UPDATE SET
			value = $3
		`,
		ks.id,
		k,
		v,
	); err != nil {
		return fmt.Errorf("cannot insert/update keyspace pair: %w", err)
	}
	return nil
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	rows, err := ks.db.QueryContext(
		ctx,
		`SELECT key, value
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1`,
		ks.id,
	)
	if err != nil {
		return fmt.Errorf("cannot query keyspace pairs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return fmt.Errorf("cannot scan keyspace pair: %w", err)
		}

		ok, err := fn(ctx, k, v)
		if !ok || err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot range over keyspace pairs: %w", err)
	}

	return nil
}

func (ks *keyspace) Close() error {
	return nil
}
