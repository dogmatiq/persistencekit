package memory_test

import (
	"net/url"
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

func TestFromURL(t *testing.T) {
	t.Run("it returns a working driver", func(t *testing.T) {
		ref := New("test-from-url")
		t.Cleanup(func() {
			ref.Close()
		})

		u := &url.URL{Scheme: "memory", Path: "/test-from-url"}
		open, err := FromURL(u)
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
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct {
			Name string
			URL  *url.URL
		}{
			{"wrong scheme", &url.URL{Scheme: "other", Path: "/silo"}},
			{"non-empty host", &url.URL{Scheme: "memory", Host: "localhost", Path: "/silo"}},
			{"query parameters", &url.URL{Scheme: "memory", Path: "/silo", RawQuery: "foo=bar"}},
			{"empty path", &url.URL{Scheme: "memory"}},
		}
		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := FromURL(tc.URL)
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		}
	})
}
