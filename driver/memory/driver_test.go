package memory_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
)

func TestNew(t *testing.T) {
	ref := New("test-new")
	t.Cleanup(func() {
		ref.Close()
	})

	d := New("test-new")
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		ref.JournalStore(),
		ref.KVStore(),
		ref.SetStore(),
	)
}

func TestParseURL(t *testing.T) {
	ref := New("test-parse-url")
	t.Cleanup(func() {
		ref.Close()
	})

	open, err := ParseURL("memory:///test-parse-url")
	if err != nil {
		t.Fatal(err)
	}

	d, err := open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		ref.JournalStore(),
		ref.KVStore(),
		ref.SetStore(),
	)
}
