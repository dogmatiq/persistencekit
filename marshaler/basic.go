package marshaler

var (
	// String marshals and unmarshals the built-in string type by performing a
	// Go type-conversion.
	String = New(
		func(v string) ([]byte, error) {
			return []byte(v), nil
		},
		func(data []byte) (string, error) {
			return string(data), nil
		},
	)

	// Bool marshals and unmarshals the built-in bool type.
	Bool = New(
		func(v bool) ([]byte, error) {
			if v {
				return []byte{1}, nil
			}
			return nil, nil
		},
		func(data []byte) (bool, error) {
			return len(data) > 0, nil
		},
	)
)
