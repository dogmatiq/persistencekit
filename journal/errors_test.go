package journal_test

import (
	"errors"
	"testing"

	. "github.com/dogmatiq/persistencekit/journal"
)

func TestIgnoreNotFound(t *testing.T) {
	err := errors.New("<error>")

	cases := []struct {
		Name     string
		Err      error
		Expected error
	}{
		{
			Name:     "ErrNotFound",
			Err:      ErrNotFound,
			Expected: nil,
		},
		{
			Name:     "ErrConflict",
			Err:      ErrConflict,
			Expected: ErrConflict,
		},
		{
			Name:     "unrecognized error",
			Err:      err,
			Expected: err,
		},
		{
			Name:     "nil error",
			Err:      nil,
			Expected: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			actual := IgnoreNotFound(c.Err)
			if actual != c.Expected {
				t.Fatalf("unexpected result: got %v, want %v", actual, c.Expected)
			}
		})
	}
}
