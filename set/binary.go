package set

// BinaryStore is a collection of sets that track membership of opaque binary
// values.
type BinaryStore = Store[[]byte]

// A BinarySet is a unique set of binary values.
//
// Values in a binary set cannot be an empty slice.
type BinarySet = Set[[]byte]

// A BinaryRangeFunc is a function used to range over members of a [BinarySet].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type BinaryRangeFunc = RangeFunc[[]byte]

// BinaryInterceptor is an [Interceptor] that can be used to intercept operations
// on a [BinarySet].
type BinaryInterceptor = Interceptor[[]byte]
