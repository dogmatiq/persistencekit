package typedjournal

// Marshaler is a constraint for types that can marshal and unmarshal journal
// records of type R.
type Marshaler[R any] interface {
	Marshal(R) ([]byte, error)
	Unmarshal([]byte) (R, error)
}
