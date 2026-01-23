package bigint

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
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
		*v.Target = T(src) ^ signBit
		return nil
	}

	return fmt.Errorf("cannot scan %T into %T", src, v.Target)
}

func (v value[T]) Value() (driver.Value, error) {
	return int64(*v.Target ^ signBit), nil
}

// signBit is the bit that is flipped to convert between signed and unsigned
// integers.
const signBit = 1 << 63
