package typedmarshaler

// Marshaler is an interface for types that can marshal and unmarshal values of
// type T.
type Marshaler[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
}

// Zero returns the zero-value of type T.
func Zero[T any]() (_ T) {
	return
}
