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

// Driver is a persistence driver backed by a named silo of in-memory stores.
// Drivers with the same silo name share state.
type Driver struct {
	name string
}

// NewDriver returns a [Driver] configured from a memory:// URL.
func NewDriver(u *url.URL) (*Driver, error) {
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

	return &Driver{name: name}, nil
}

// JournalStore returns the silo's in-memory journal store.
func (d *Driver) JournalStore(context.Context) (journal.BinaryStore, error) {
	return &d.load().journal, nil
}

// KVStore returns the silo's in-memory key/value store.
func (d *Driver) KVStore(context.Context) (kv.BinaryStore, error) {
	return &d.load().kv, nil
}

// SetStore returns the silo's in-memory set store.
func (d *Driver) SetStore(context.Context) (set.BinaryStore, error) {
	return &d.load().set, nil
}

// Close is a no-op. The silo's state persists for the lifetime of the process.
func (d *Driver) Close() error {
	return nil
}

func (d *Driver) load() *silo {
	v, _ := silos.LoadOrStore(d.name, &silo{})
	return v.(*silo)
}
