package marshaler

// Marshaler is an interface for types that can marshal and unmarshal values of
// type T.
type Marshaler[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
}

// New returns a new [Marshaler] that marshals and unmarshals values of type T
// using the given functions.
func New[T any](
	marshal func(T) ([]byte, error),
	unmarshal func([]byte) (T, error),
) Marshaler[T] {
	return marshaler[T]{marshal, unmarshal}
}

type marshaler[T any] struct {
	marshal   func(T) ([]byte, error)
	unmarshal func([]byte) (T, error)
}

func (m marshaler[T]) Marshal(v T) ([]byte, error)      { return m.marshal(v) }
func (m marshaler[T]) Unmarshal(data []byte) (T, error) { return m.unmarshal(data) }
