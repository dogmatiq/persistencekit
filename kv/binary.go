package kv

// BinaryStore is a collection of keyspaces that map opaque binary keys to
// binary values.
type BinaryStore = Store[[]byte, []byte]

// A BinaryKeyspace is an isolated collection of binary key/value pairs.
//
// Keys in a binary keyspace cannot be an empty slice.
type BinaryKeyspace = Keyspace[[]byte, []byte]

// A BinaryRangeFunc is a function used to range over the key/value pairs in a
// [BinaryKeyspace].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type BinaryRangeFunc = RangeFunc[[]byte, []byte]
