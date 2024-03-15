package pgerror

import (
	"errors"
	"slices"

	"github.com/jackc/pgconn"
)

// https://www.postgresql.org/docs/11/errcodes-appendix.html
const (
	// CodeUniqueViolation is the PostgreSQL error code for "unique_violation".
	CodeUniqueViolation = "23505"

	// CodeUndefinedTable is the PostgreSQL error code for "undefined_table".
	CodeUndefinedTable = "42P01"
)

// Is returns true if err is a PostgreSQL error with one of the given codes.
func Is(err error, codes ...string) bool {
	var e *pgconn.PgError
	return errors.As(err, &e) && slices.Contains(codes, e.Code)
}
