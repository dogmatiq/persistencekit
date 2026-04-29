// Package drivertest provides test helpers that verify a [Driver] provides
// access to the expected stores.
package drivertest

import (
	"bytes"
	"testing"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver is the subset of [persistencekit.Driver] used by driver tests.
type Driver interface {
	JournalStore() journal.BinaryStore
	KVStore() kv.BinaryStore
	SetStore() set.BinaryStore
}

// RunTests verifies that the driver's stores share the same data as the given
// reference stores by writing through the driver and reading through the
// reference stores.
func RunTests(
	t *testing.T,
	d Driver,
	journalStore journal.BinaryStore,
	kvStore kv.BinaryStore,
	setStore set.BinaryStore,
) {
	t.Run("JournalStore", func(t *testing.T) {
		t.Parallel()
		testJournalStore(t, d.JournalStore(), journalStore)
	})

	t.Run("KVStore", func(t *testing.T) {
		t.Parallel()
		testKVStore(t, d.KVStore(), kvStore)
	})

	t.Run("SetStore", func(t *testing.T) {
		t.Parallel()
		testSetStore(t, d.SetStore(), setStore)
	})
}

func testJournalStore(t *testing.T, writer, reader journal.BinaryStore) {
	ctx := t.Context()

	w, err := writer.Open(ctx, "journal")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	rec := []byte("<record>")
	if err := w.Append(ctx, 0, rec); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open(ctx, "journal")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.Get(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, rec) {
		t.Fatalf("journal record mismatch: got %q, want %q", got, rec)
	}
}

func testKVStore(t *testing.T, writer, reader kv.BinaryStore) {
	ctx := t.Context()

	w, err := writer.Open(ctx, "keyspace")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := w.Set(ctx, []byte("<key>"), []byte("<value>"), ""); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open(ctx, "keyspace")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, _, err := r.Get(ctx, []byte("<key>"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, []byte("<value>")) {
		t.Fatalf("kv value mismatch: got %q, want %q", got, "<value>")
	}
}

func testSetStore(t *testing.T, writer, reader set.BinaryStore) {
	ctx := t.Context()

	w, err := writer.Open(ctx, "set")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Add(ctx, []byte("<member>")); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open(ctx, "set")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ok, err := r.Has(ctx, []byte("<member>"))
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("set member not found via reader")
	}
}
