package commonschema

import (
	"database/sql/driver"
	"fmt"
	"math"
)

// Uint64 is a uint64 that is represented as a SIGNED BIGINT (64-bit)
// integer in PostgreSQL (which is PostgreSQL's largest integer type).
//
// The encoding preserves order, such that sorting on the encoded values will
// produce the same order as sorting on the original unsigned values. This is
// the only guarantee provided by this type.
type Uint64 uint64

// Scan implements [sql.Scanner].
func (p *Uint64) Scan(src any) error {
	if src, ok := src.(int64); ok {
		*p = Uint64(uint64(src) + uint64(math.MaxInt64) + 1)
		return nil
	}

	return fmt.Errorf("cannot scan %T into bigint.Unsigned", src)
}

// Value implements [driver.Valuer].
func (p Uint64) Value() (driver.Value, error) {
	return int64(p - (math.MaxInt64 + 1)), nil
}
