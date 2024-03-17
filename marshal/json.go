package marshal

import "encoding/json"

// JSON is a [Marshaler] that uses the JSON encoding format.
type JSON[T any] struct{}

// Marshal returns the JSON representation of v.
func (JSON[T]) Marshal(v T) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal returns a value of type T constructed from its JSON representation.
func (JSON[T]) Unmarshal(data []byte) (T, error) {
	var v T
	return v, json.Unmarshal(data, &v)
}
