package kv

// BinaryStore is a collection of keyspaces that map opaque binary keys to
// binary values.
type BinaryStore = Store[[]byte, []byte]

// A BinaryKeyspace is an isolated collection of binary key/value pairs.
type BinaryKeyspace = Keyspace[[]byte, []byte]

type BinaryRangeFunc = RangeFunc[[]byte, []byte]
