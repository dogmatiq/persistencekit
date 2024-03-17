package marshal

// Marshaler is an interface for types that can marshal and unmarshal values of
// type T.
type Marshaler[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
}
