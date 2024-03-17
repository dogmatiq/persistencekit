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

var (
	// String marshals and unmarshals the built-in string type.
	String Marshaler[string] = Convert[string]{}

	// Bool marshals and unmarshals the built-in bool type.
	Bool Marshaler[bool] = convertBool{}
)

type convertBool struct{}

func (convertBool) Marshal(v bool) ([]byte, error) {
	if v {
		return []byte{1}, nil
	}
	return nil, nil
}

func (convertBool) Unmarshal(data []byte) (bool, error) {
	return len(data) > 0, nil
}
