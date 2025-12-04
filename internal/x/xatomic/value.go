package xatomic

import "sync/atomic"

// Value provides an atomic load and store of a value of type T.
type Value[T any] struct {
	p atomic.Pointer[T]
}

// Load atomically loads and returns the value stored in x.
// If no value has been stored it returns the zero value of T.
func (x *Value[T]) Load() T {
	ptr := x.p.Load()
	if ptr == nil {
		var zero T
		return zero
	}
	return *ptr
}

// Store atomically stores val into x.
func (x *Value[T]) Store(val T) {
	x.p.Store(&val)
}
