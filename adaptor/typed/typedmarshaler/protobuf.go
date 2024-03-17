package typedmarshaler

import (
	"google.golang.org/protobuf/proto"
)

// ProtocolBuffers is a [Marshaler] that encodes/decodes protocol buffers
// messages.
type ProtocolBuffers[T proto.Message] struct{}

// Marshal returns the protocol buffers representation of v.
func (ProtocolBuffers[T]) Marshal(v T) ([]byte, error) {
	return proto.Marshal(v)
}

// Unmarshal returns a value of type T constructed from its protocol buffers
// representation.
func (ProtocolBuffers[T]) Unmarshal(data []byte) (T, error) {
	var v T
	v = v.ProtoReflect().New().(T)
	return v, proto.Unmarshal(data, v)
}
