package kv

import (
	"context"
	"sync/atomic"
)

// Interceptor defines functions that are invoked around keyspace operations.
type Interceptor[K, V any] struct {
	beforeOpen atomic.Pointer[func(string) error]
	beforeSet  atomic.Pointer[func(string, K, V) error]
	afterSet   atomic.Pointer[func(string, K, V) error]
}

// BeforeOpen sets the function that is invoked before a [Keyspace] is opened.
func (i *Interceptor[K, V]) BeforeOpen(fn func(name string) error) {
	setOpenFn(&i.beforeOpen, fn)
}

// BeforeSet sets the function that is invoked before a key/value pair is set.
func (i *Interceptor[K, V]) BeforeSet(fn func(keyspace string, k K, v V) error) {
	setMutationFn(&i.beforeSet, fn)
}

// AfterSet sets the function that is invoked after a key/value pair is set.
func (i *Interceptor[K, V]) AfterSet(fn func(keyspace string, k K, v V) error) {
	setMutationFn(&i.afterSet, fn)
}

// WithInterceptor returns a [Store] that invokes the functions defined by the
// given [Interceptor] when performing operations on s.
func WithInterceptor[K, V any](s Store[K, V], in *Interceptor[K, V]) Store[K, V] {
	if in == nil {
		return s
	}

	return &interceptedStore[K, V]{
		Next:        s,
		Interceptor: in,
	}
}

func setOpenFn(dst *atomic.Pointer[func(string) error], fn func(string) error) {
	if fn == nil {
		dst.Store(nil)
		return
	}

	dst.Store(&fn)
}

func setMutationFn[K, V any](dst *atomic.Pointer[func(string, K, V) error], fn func(string, K, V) error) {
	if fn == nil {
		dst.Store(nil)
		return
	}

	dst.Store(&fn)
}

type interceptedStore[K, V any] struct {
	Next        Store[K, V]
	Interceptor *Interceptor[K, V]
}

func (s *interceptedStore[K, V]) Open(ctx context.Context, name string) (Keyspace[K, V], error) {
	if fn := s.Interceptor.beforeOpenFn(); fn != nil {
		if err := fn(name); err != nil {
			return nil, err
		}
	}

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &interceptedKeyspace[K, V]{
		Next:        next,
		keyspace:    next.Name(),
		Interceptor: s.Interceptor,
	}, nil
}

type interceptedKeyspace[K, V any] struct {
	Next        Keyspace[K, V]
	keyspace    string
	Interceptor *Interceptor[K, V]
}

func (ks *interceptedKeyspace[K, V]) Name() string {
	return ks.Next.Name()
}

func (ks *interceptedKeyspace[K, V]) Get(ctx context.Context, k K) (V, error) {
	return ks.Next.Get(ctx, k)
}

func (ks *interceptedKeyspace[K, V]) Has(ctx context.Context, k K) (bool, error) {
	return ks.Next.Has(ctx, k)
}

func (ks *interceptedKeyspace[K, V]) Set(ctx context.Context, k K, v V) error {
	if fn := ks.Interceptor.beforeSetFn(); fn != nil {
		if err := fn(ks.keyspace, k, v); err != nil {
			return err
		}
	}

	if err := ks.Next.Set(ctx, k, v); err != nil {
		return err
	}

	if fn := ks.Interceptor.afterSetFn(); fn != nil {
		if err := fn(ks.keyspace, k, v); err != nil {
			return err
		}
	}

	return nil
}

func (ks *interceptedKeyspace[K, V]) Range(ctx context.Context, fn RangeFunc[K, V]) error {
	return ks.Next.Range(ctx, fn)
}

func (ks *interceptedKeyspace[K, V]) Close() error {
	return ks.Next.Close()
}

func (i *Interceptor[K, V]) beforeOpenFn() func(string) error {
	if i == nil {
		return nil
	}

	if fn := i.beforeOpen.Load(); fn != nil {
		return *fn
	}

	return nil
}

func (i *Interceptor[K, V]) beforeSetFn() func(string, K, V) error {
	if i == nil {
		return nil
	}

	if fn := i.beforeSet.Load(); fn != nil {
		return *fn
	}

	return nil
}

func (i *Interceptor[K, V]) afterSetFn() func(string, K, V) error {
	if i == nil {
		return nil
	}

	if fn := i.afterSet.Load(); fn != nil {
		return *fn
	}

	return nil
}
