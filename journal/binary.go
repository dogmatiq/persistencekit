package journal

// BinaryStore is a [Store] of journals that contain opaque binary records.
type BinaryStore = Store[[]byte]

// A BinaryJournal is an append-only log of binary records.
type BinaryJournal = Journal[[]byte]

// A BinaryRangeFunc is a function used to range over the records in a
// [BinaryJournal].
//
// If err is non-nil, ranging stops and err is propagated up the stack.
// Otherwise, if ok is false, ranging stops without any error being propagated.
type BinaryRangeFunc = RangeFunc[[]byte]
