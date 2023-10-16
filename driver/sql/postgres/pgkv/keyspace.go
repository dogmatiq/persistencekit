package pgkv

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/persistencekit/kv"
)

type keyspace struct {
	Name string
	DB   *sql.DB
}

func (ks *keyspace) Get(ctx context.Context, k []byte) (v []byte, err error) {
	row := ks.DB.QueryRowContext(
		ctx,
		`SELECT
			value
		FROM persistencekit.kv
		WHERE keyspace = $1
		AND key = $2`,
		ks.Name,
		k,
	)

	var value []byte
	err = row.Scan(&value)
	if err == sql.ErrNoRows {
		err = nil
	}

	return value, err
}

func (ks *keyspace) Has(ctx context.Context, k []byte) (ok bool, err error) {
	row := ks.DB.QueryRowContext(
		ctx,
		`SELECT
			1
		FROM persistencekit.kv
		WHERE keyspace = $1
		AND key = $2`,
		ks.Name,
		k,
	)

	var value []byte
	err = row.Scan(&value)
	if err == sql.ErrNoRows {
		return false, nil
	}

	return true, err
}

func (ks *keyspace) Set(ctx context.Context, k, v []byte) error {
	if len(v) == 0 {
		_, err := ks.DB.ExecContext(
			ctx,
			`DELETE FROM persistencekit.kv
			WHERE keyspace = $1
			AND key = $2`,
			ks.Name,
			k,
		)

		return err
	}

	_, err := ks.DB.ExecContext(
		ctx,
		`INSERT INTO persistencekit.kv AS o (
			keyspace,
			key,
			value
		) VALUES (
			$1, $2, $3
		) ON CONFLICT (keyspace, key) DO UPDATE SET
			value = $3
		`,
		ks.Name,
		k,
		v,
	)

	return err
}

func (ks *keyspace) Range(
	ctx context.Context,
	fn kv.RangeFunc,
) error {
	rows, err := ks.DB.QueryContext(
		ctx,
		`SELECT
			key,
			value
		FROM persistencekit.kv
		WHERE keyspace = $1`,
		ks.Name,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			k []byte
			v []byte
		)
		if err := rows.Scan(&k, &v); err != nil {
			return err
		}

		ok, err := fn(ctx, k, v)
		if !ok || err != nil {
			return err
		}
	}

	return rows.Err()
}

func (ks *keyspace) Close() error {
	return nil
}
