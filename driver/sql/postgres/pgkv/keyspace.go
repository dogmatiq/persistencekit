package pgkv

import (
	"context"
	"database/sql"
	"encoding/binary"
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

func (ks *keyspace) Get(ctx context.Context, k []byte) (v, t []byte, err error) {
	row := ks.db.QueryRowContext(
		ctx,
		`SELECT value, generation
		FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1
		AND key = $2`,
		ks.id,
		k,
	)

	var (
		value      []byte
		generation uint64
	)

	if err := row.Scan(&value, &generation); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("cannot scan keyspace pair: %w", err)
	}

	return value, binary.BigEndian.AppendUint64(nil, generation), nil
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

func (ks *keyspace) Set(ctx context.Context, k, v, t []byte) ([]byte, error) {
	generation, ok := unmarshalToken(t)
	if !ok {
		return nil, kv.ConflictError[[]byte]{
			Keyspace: ks.name,
			Key:      k,
			Token:    t,
		}
	}

	var err error

	if len(v) == 0 {
		ok, err = ks.delete(ctx, k, generation)
		generation = 0
	} else if generation == 0 {
		ok, err = ks.insert(ctx, k, v)
		generation++
	} else {
		ok, err = ks.update(ctx, k, v, generation)
		generation++
	}

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, kv.ConflictError[[]byte]{
			Keyspace: ks.name,
			Key:      k,
			Token:    t,
		}
	}

	return marshalToken(generation), nil
}

func (ks *keyspace) insert(ctx context.Context, k, v []byte) (bool, error) {
	res, err := ks.db.ExecContext(
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
	if err != nil {
		return false, fmt.Errorf("cannot insert keyspace pair: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot determine affected rows: %w", err)
	}

	return n == 1, nil
}

func (ks *keyspace) update(ctx context.Context, k, v []byte, generation uint64) (bool, error) {
	res, err := ks.db.ExecContext(
		ctx,
		`UPDATE persistencekit.keyspace_pair SET
			value = $3,
			generation = generation + 1
		WHERE keyspace_id = $1
		AND key = $2
		AND generation = $4`,
		ks.id,
		k,
		v,
		generation,
	)
	if err != nil {
		return false, fmt.Errorf("cannot update keyspace pair: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot determine affected rows: %w", err)
	}

	return n == 1, nil
}

func (ks *keyspace) delete(ctx context.Context, k []byte, generation uint64) (bool, error) {
	if generation == 0 {
		// We're attempting to delete a key that we expect does not exist.
		// This is a no-op on success, but we still need to check for conflicts.
		row := ks.db.QueryRowContext(
			ctx,
			`SELECT COUNT(*) = 0
			FROM persistencekit.keyspace_pair
			WHERE keyspace_id = $1
			AND key = $2`,
			ks.id,
			k,
		)

		var ok bool
		err := row.Scan(&ok)
		return ok, err
	}

	res, err := ks.db.ExecContext(
		ctx,
		`DELETE FROM persistencekit.keyspace_pair
		WHERE keyspace_id = $1
		AND key = $2
		AND generation = $3`,
		ks.id,
		k,
		generation,
	)
	if err != nil {
		return false, fmt.Errorf("cannot delete keyspace pair: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot determine affected rows: %w", err)
	}

	return n == 1, nil
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	rows, err := ks.db.QueryContext(
		ctx,
		`SELECT key, value, generation
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
			k, v       []byte
			generation uint64
		)
		if err := rows.Scan(&k, &v, &generation); err != nil {
			return fmt.Errorf("cannot scan keyspace pair: %w", err)
		}

		var token [8]byte
		binary.BigEndian.PutUint64(token[:], generation)

		ok, err := fn(ctx, k, v, token[:])
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

func marshalToken(g uint64) []byte {
	if g == 0 {
		return nil
	}
	return binary.BigEndian.AppendUint64(nil, g)
}

func unmarshalToken(t []byte) (uint64, bool) {
	switch len(t) {
	case 0:
		return 0, true
	case 8:
		generation := binary.BigEndian.Uint64(t)
		return binary.BigEndian.Uint64(t), generation != 0
	default:
		return 0, false
	}
}
