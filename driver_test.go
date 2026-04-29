package persistencekit_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit"
)

func TestParseURL(t *testing.T) {
	t.Run("it returns an open function for a valid memory URL", func(t *testing.T) {
		open, err := ParseURL("memory:///test-silo")
		if err != nil {
			t.Fatal(err)
		}

		d, err := open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
	})

	t.Run("it returns an open function for a valid postgres URL", func(t *testing.T) {
		_, err := ParseURL("postgres://user:pass@localhost/db")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it returns an open function for a valid postgresql URL", func(t *testing.T) {
		_, err := ParseURL("postgresql://user:pass@localhost/db")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it returns an open function for a valid dynamodb URL", func(t *testing.T) {
		_, err := ParseURL("dynamodb:///prefix")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it returns an open function for a valid s3 URL", func(t *testing.T) {
		_, err := ParseURL("s3:///bucket")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it returns an error when the URL has no scheme", func(t *testing.T) {
		_, err := ParseURL("no-scheme")
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error when the URL has an unsupported scheme", func(t *testing.T) {
		_, err := ParseURL("redis://localhost")
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error when the URL is malformed", func(t *testing.T) {
		_, err := ParseURL("://")
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}
