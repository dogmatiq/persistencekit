package marshaler

import (
	"google.golang.org/protobuf/proto"
)

// NewProto returns a marshaler that marshals and unmarshals Protocol Buffers
// messages.
func NewProto[
	T interface {
		proto.Message
		*S
	},
	S any,
]() Marshaler[T] {
	return marshaler[T]{
		func(t T) ([]byte, error) {
			return proto.Marshal(t)
		},
		func(data []byte) (T, error) {
			var v T = new(S)
			return v, proto.Unmarshal(data, v)
		},
	}
}
