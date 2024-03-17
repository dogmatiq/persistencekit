package marshal

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
