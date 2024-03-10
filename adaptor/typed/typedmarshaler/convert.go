package typedmarshaler

// Identity is the "identity" marshaler, it does not perform any conversion.
type Identity = Convert[[]byte]

// String is a [Marshaler] that performs type conversion between []byte and
// string without changing the underlying data.
type String = Convert[string]

// Convert is a [Marshaler] that performs a type conversion between types
// without changing the underlying data.
type Convert[T ~string | ~[]byte] struct{}

// Marshal returns v unmodified.
func (Convert[T]) Marshal(v T) ([]byte, error) {
	return []byte(v), nil
}

// Unmarshal returns data unmodified.
func (Convert[T]) Unmarshal(data []byte) (T, error) {
	return T(data), nil
}
