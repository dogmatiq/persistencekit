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

func (ks *keyspace) Get(ctx context.Context, k []byte) (v []byte, r uint64, err error) {
	row := ks.db.QueryRowContext(
		ctx,
		`SELECT value, revision
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1
		AND key = $2`,
		ks.id,
		k,
	)

	if err := row.Scan(&v, &r); err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("cannot scan keyspace pair: %w", err)
	}

	return v, r, nil
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

func (ks *keyspace) Set(ctx context.Context, k, v []byte, r uint64) error {
	ok, err := ks.set(ctx, v, k, r)
	if ok || err != nil {
		return err
	}

	return kv.ConflictError[[]byte]{
		Keyspace: ks.name,
		Key:      k,
		Revision: r,
	}
}

// set inserts, updates, or deletes a key/value pair based on the provided
// value and revision.
//
// It returns true on success, or false on conflict.
func (ks *keyspace) set(ctx context.Context, v []byte, k []byte, r uint64) (bool, error) {
	isDelete := len(v) == 0
	isNew := r == 0

	if isDelete && isNew {
		exists, err := ks.Has(ctx, k)
		return !exists, err
	}

	if isDelete {
		return ks.execOne(
			ctx,
			`DELETE FROM persistencekit.keyspace_pair
			WHERE keyspace_id = $1
			AND key = $2
			AND revision = $3`,
			ks.id,
			k,
			r,
		)
	}

	if isNew {
		return ks.execOne(
			ctx,
			`INSERT INTO persistencekit.keyspace_pair AS o (
				keyspace_id,
				key,
				value
			) VALUES (
				$1, $2, $3
			) ON CONFLICT (keyspace_id, key) DO NOTHING`,
			ks.id,
			k,
			v,
		)
	}

	return ks.execOne(
		ctx,
		`UPDATE persistencekit.keyspace_pair SET
			value = $3,
			revision = revision + 1
		WHERE keyspace_id = $1
		AND key = $2
		AND revision = $4`,
		ks.id,
		k,
		v,
		r,
	)
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	rows, err := ks.db.QueryContext(
		ctx,
		`SELECT key, value, revision
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1`,
		ks.id,
	)
	if err != nil {
		return fmt.Errorf("cannot query keyspace pairs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			k, v []byte
			r    uint64
		)
		if err := rows.Scan(&k, &v, &r); err != nil {
			return fmt.Errorf("cannot scan keyspace pair: %w", err)
		}

		ok, err := fn(ctx, k, v, r)
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

// execOne executes a query and returns whether exactly one row was affected.
func (ks *keyspace) execOne(
	ctx context.Context,
	query string,
	args ...any,
) (bool, error) {
	res, err := ks.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("cannot execute query: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot determine affected rows: %w", err)
	}

	return n == 1, nil
}
