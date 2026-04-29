package persistencekit

import (
	"errors"
	"fmt"
)

// IsUnsupported reports whether err indicates that a driver does not support a
// particular persistence primitive.
func IsUnsupported(err error) bool {
	_, ok := errors.AsType[unsupportedError](err)
	return ok
}

// unsupportedError is returned by a [Driver] when it does not support a
// particular persistence primitive.
type unsupportedError struct {
	Driver    string
	Primitive string
}

func (e unsupportedError) Error() string {
	return fmt.Sprintf("the %s driver does not support %s stores", e.Driver, e.Primitive)
}
