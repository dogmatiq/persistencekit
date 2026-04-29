package memory

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

var silos sync.Map

type silo struct {
	kv      memorykv.BinaryStore
	journal memoryjournal.BinaryStore
	set     memoryset.BinaryStore
}

// Driver is a persistence driver backed by a named in-memory silo.
type Driver struct {
	silo *silo
}

// New returns a [Driver] backed by the named silo.
func New(name string) *Driver {
	v, _ := silos.LoadOrStore(name, &silo{})
	return &Driver{silo: v.(*silo)}
}

// ParseURL returns a function that opens a [Driver] configured by the given
// memory:// URL string.
//
// URL format:
//
//	memory:///<silo>
func ParseURL(u string) (func(context.Context) (*Driver, error), error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid memory URL: %w", err)
	}
	return FromURL(parsed)
}

// FromURL returns a function that opens a [Driver] configured by the given
// memory:// [*url.URL]. See [ParseURL] for the URL format.
func FromURL(u *url.URL) (func(context.Context) (*Driver, error), error) {
	if u.Scheme != "memory" {
		return nil, fmt.Errorf("invalid memory URL: unexpected scheme %q", u.Scheme)
	}

	if u.Host != "" {
		return nil, fmt.Errorf("invalid memory URL: host component must be empty, use memory:///<silo> for a named silo")
	}

	if u.RawQuery != "" {
		return nil, fmt.Errorf("invalid memory URL: query parameters are not supported")
	}

	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return nil, fmt.Errorf("invalid memory URL: silo name is required in the URL path: memory:///<silo>")
	}

	return func(context.Context) (*Driver, error) {
		return New(name), nil
	}, nil
}

// JournalStore returns the silo's in-memory journal store.
func (d *Driver) JournalStore() journal.BinaryStore {
	return &d.silo.journal
}

// KVStore returns the silo's in-memory key/value store.
func (d *Driver) KVStore() kv.BinaryStore {
	return &d.silo.kv
}

// SetStore returns the silo's in-memory set store.
func (d *Driver) SetStore() set.BinaryStore {
	return &d.silo.set
}

// Close is a no-op. The silo's state persists for the lifetime of the process.
func (d *Driver) Close() error {
	return nil
}
