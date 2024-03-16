package bigint

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
)

// ConvertUnsigned returns a type that can be used in SQL statements and scan
// operations that encodes an unsigned 64-bit integer as a signed 64-bit integer
// (which is PostgreSQL's largest integer type).
//
// The encoding preserves order, such that sorting on the encoded values will
// produce the same order as sorting on the original unsigned values. This is
// the only guarantee provided by this type.
func ConvertUnsigned[T ~uint64](target *T) interface {
	driver.Valuer
	sql.Scanner
} {
	return value[T]{target}
}

type value[T ~uint64] struct {
	Target *T
}

func (v value[T]) Scan(src any) error {
	if src, ok := src.(int64); ok {
		unmarshal(src, v.Target)
		return nil
	}

	return fmt.Errorf("cannot scan %T into journal.Position", src)
}

func (v value[T]) Value() (driver.Value, error) {
	return marshal(*v.Target), nil
}

func marshal[T ~uint64](target T) int64 {
	return int64(target - (math.MaxInt64 + 1))
}

func unmarshal[T ~uint64](src int64, target *T) {
	*target = T(src) + T(math.MaxInt64) + 1
}
