package pgkv

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/bigint"
	"github.com/dogmatiq/persistencekit/internal/kvrevision"
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

func (ks *keyspace) Get(ctx context.Context, k []byte) (v []byte, r kv.Revision, err error) {
	row := ks.db.QueryRowContext(
		ctx,
		`SELECT value, encoded_generation
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1
		AND key = $2`,
		ks.id,
		k,
	)

	var gen uint64
	if err := row.Scan(
		&v,
		bigint.ConvertUnsigned(&gen),
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("cannot scan keyspace pair: %w", err)
	}

	return v, kvrevision.MarshalGeneration(gen), nil
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

func (ks *keyspace) Set(ctx context.Context, k, v []byte, r kv.Revision) (kv.Revision, error) {
	next, ok, err := ks.set(ctx, v, k, r)
	if ok || err != nil {
		return next, err
	}

	return "", kv.ConflictError[[]byte]{
		Keyspace: ks.name,
		Key:      k,
		Revision: r,
	}
}

// set inserts, updates, or deletes a key/value pair based on the provided value
// and revision.
//
// It returns the new revision and true on success, or empty and false on
// conflict.
func (ks *keyspace) set(ctx context.Context, v []byte, k []byte, r kv.Revision) (kv.Revision, bool, error) {
	isDelete := len(v) == 0
	isNew := r == ""

	if isDelete && isNew {
		exists, err := ks.Has(ctx, k)
		return "", !exists, err
	}

	gen, ok := kvrevision.TryUnmarshalGeneration(r)
	if !ok {
		return "", false, nil
	}

	if isDelete {
		ok, err := ks.execOne(
			ctx,
			`DELETE FROM persistencekit.keyspace_pair
			WHERE keyspace_id = $1
			AND key = $2
			AND encoded_generation = $3`,
			ks.id,
			k,
			bigint.ConvertUnsigned(&gen),
		)
		return "", ok, err
	}

	if isNew {
		ok, err := ks.execOne(
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
		return kvrevision.MarshalGeneration(1), ok, err
	}

	ok, err := ks.execOne(
		ctx,
		`UPDATE persistencekit.keyspace_pair SET
			value = $3,
			encoded_generation = encoded_generation + 1
		WHERE keyspace_id = $1
			AND key = $2
			AND encoded_generation = $4`,
		ks.id,
		k,
		v,
		bigint.ConvertUnsigned(&gen),
	)

	return kvrevision.MarshalGeneration(gen + 1), ok, err
}

func (ks *keyspace) SetUnconditional(ctx context.Context, k, v []byte) error {
	if len(v) == 0 {
		_, err := ks.db.ExecContext(
			ctx,
			`DELETE FROM persistencekit.keyspace_pair
			WHERE keyspace_id = $1
			AND key = $2`,
			ks.id,
			k,
		)
		return err
	}

	_, err := ks.db.ExecContext(
		ctx,
		`INSERT INTO persistencekit.keyspace_pair AS p (
			keyspace_id,
			key,
			value
		) VALUES (
			$1, $2, $3
		) ON CONFLICT (keyspace_id, key) DO UPDATE SET
			value = EXCLUDED.value,
			encoded_generation = p.encoded_generation + 1`,
		ks.id,
		k,
		v,
	)
	return err
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	rows, err := ks.db.QueryContext(
		ctx,
		`SELECT key, value, encoded_generation
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
			gen  uint64
		)
		if err := rows.Scan(
			&k,
			&v,
			bigint.ConvertUnsigned(&gen),
		); err != nil {
			return fmt.Errorf("cannot scan keyspace pair: %w", err)
		}

		ok, err := fn(ctx, k, v, kvrevision.MarshalGeneration(gen))
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
