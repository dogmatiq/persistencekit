package set

// BinaryStore is a collection of sets that track membership of opaque binary
// values.
type BinaryStore = Store[[]byte]

// A BinarySet is a unique set of binary values.
//
// Values in a binary set cannot be an empty slice.
type BinarySet = Set[[]byte]
