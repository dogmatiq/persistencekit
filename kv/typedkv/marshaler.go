package typedkv

// Marshaler is a constraint for types that can marshal and unmarshal values of
// type T.
type Marshaler[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
}
