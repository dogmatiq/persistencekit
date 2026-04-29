package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Driver is a persistence driver backed by PostgreSQL.
type Driver struct {
	config *pgxpool.Config

	m    sync.Mutex
	pool *pgxpool.Pool
	db   *sql.DB
}

// NewDriver returns a [Driver] configured from a postgres:// or postgresql://
// URL.
//
// Pool settings can be configured via URL parameters. See
// [pgxpool.ParseConfig] for the full list of supported parameters.
func NewDriver(u *url.URL) (*Driver, error) {
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}

	return &Driver{config: cfg}, nil
}

// JournalStore returns a journal store backed by PostgreSQL.
func (d *Driver) JournalStore(ctx context.Context) (journal.BinaryStore, error) {
	db, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return &pgjournal.BinaryStore{DB: db}, nil
}

// KVStore returns a key/value store backed by PostgreSQL.
func (d *Driver) KVStore(ctx context.Context) (kv.BinaryStore, error) {
	db, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return &pgkv.BinaryStore{DB: db}, nil
}

// SetStore returns a set store backed by PostgreSQL.
func (d *Driver) SetStore(ctx context.Context) (set.BinaryStore, error) {
	db, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return &pgset.BinaryStore{DB: db}, nil
}

// Close closes the underlying connection pool.
func (d *Driver) Close() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.config = nil

	if d.db == nil {
		return nil
	}

	err := d.db.Close()
	d.db = nil

	d.pool.Close()
	d.pool = nil

	return err
}

func (d *Driver) open(ctx context.Context) (*sql.DB, error) {
	d.m.Lock()
	defer d.m.Unlock()

	if d.db != nil {
		return d.db, nil
	}

	if d.config == nil {
		panic("driver is closed")
	}

	pool, err := pgxpool.NewWithConfig(ctx, d.config)
	if err != nil {
		return nil, err
	}

	d.pool = pool
	d.db = stdlib.OpenDBFromPool(pool)

	return d.db, nil
}
