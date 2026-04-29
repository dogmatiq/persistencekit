package persistencekit_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit"
)

func TestNewDriver(t *testing.T) {
	t.Run("it returns a driver for a valid memory URL", func(t *testing.T) {
		d, err := NewDriver("memory:///test-silo")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
	})

	t.Run("it returns a driver for a valid postgres URL", func(t *testing.T) {
		d, err := NewDriver("postgres://user:pass@localhost/db")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
	})

	t.Run("it returns a driver for a valid postgresql URL", func(t *testing.T) {
		d, err := NewDriver("postgresql://user:pass@localhost/db")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
	})

	t.Run("it returns a driver for a valid dynamodb URL", func(t *testing.T) {
		d, err := NewDriver("dynamodb:///prefix")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
	})

	t.Run("it returns a driver for a valid s3 URL", func(t *testing.T) {
		d, err := NewDriver("s3:///bucket")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
	})

	t.Run("it returns an error when the URL has no scheme", func(t *testing.T) {
		_, err := NewDriver("no-scheme")
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error when the URL has an unsupported scheme", func(t *testing.T) {
		_, err := NewDriver("redis://localhost")
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error when the URL is malformed", func(t *testing.T) {
		_, err := NewDriver("://")
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}
