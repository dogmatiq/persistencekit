package marshaler

import "encoding/json"

// NewJSON returns a marshaler that marshals and unmarshals an arbitrary type using
// Go's standard NewJSON encoding.
func NewJSON[T any]() Marshaler[T] {
	return marshaler[T]{
		func(v T) ([]byte, error) {
			return json.Marshal(v)
		},
		func(data []byte) (T, error) {
			var v T
			return v, json.Unmarshal(data, &v)
		},
	}
}
