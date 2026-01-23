package kv

import (
	"context"

	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/internal/x/xtelemetry"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WithTelemetry returns a [BinaryStore] that adds telemetry to s.
func WithTelemetry(
	s BinaryStore,
	p trace.TracerProvider,
	m metric.MeterProvider,
	l log.LoggerProvider,
) BinaryStore {
	return &instrumentedStore{
		Next: s,
		Telemetry: telemetry.Provider{
			TracerProvider: p,
			MeterProvider:  m,
			LoggerProvider: l,
		},
	}
}

// instrumentedStore is a decorator that adds instrumentation to a [BinaryStore].
type instrumentedStore struct {
	Next      BinaryStore
	Telemetry telemetry.Provider
}

// Open returns the keyspace with the given name.
func (s *instrumentedStore) Open(ctx context.Context, name string) (BinaryKeyspace, error) {
	telem := s.Telemetry.Recorder(
		"github.com/dogmatiq/persistencekit/kv",
		telemetry.Type("kv.store", s.Next),
		telemetry.String("keyspace.name", name),
		telemetry.String("keyspace.handle", xtelemetry.HandleID()),
	)

	ks := &instrumentedKeyspace{
		Telemetry:     telem,
		OpenKeyspaces: telem.UpDownCounter("open_keyspaces", "{keyspace}", "The number of keyspaces that are currently open."),
		Conflicts:     telem.Counter("conflicts", "{error}", "The number of times setting a value has failed due to an optimistic-concurrency conflict."),
		Misses:        telem.Counter("misses", "{operation}", "The number of times the value associated with a specific key was requested but not present in the keyspace."),
		KeyIO:         telem.Counter("key.io", "By", "The cumulative size of the keys that have been operated upon."),
		ValueIO:       telem.Counter("value.io", "By", "The cumulative size of the values that have been operated upon."),
		KeySize:       telem.Histogram("key.size", "By", "The sizes of the keys that have been operated upon."),
		ValueSize:     telem.Histogram("value.size", "By", "The sizes of the values that have been operated upon."),
	}

	ctx, span := telem.StartSpan(ctx, "keyspace.open")
	defer span.End()

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.open.error", "unable to open keyspace", err)
		return nil, err
	}

	ks.Next = next

	ks.OpenKeyspaces(ctx, 1)
	ks.Telemetry.Info(ctx, "keyspace.open.ok", "opened keyspace")

	return ks, nil
}

type instrumentedKeyspace struct {
	Next      BinaryKeyspace
	Telemetry *telemetry.Recorder

	OpenKeyspaces telemetry.Instrument[int64]
	Conflicts     telemetry.Instrument[int64]
	Misses        telemetry.Instrument[int64]
	KeyIO         telemetry.Instrument[int64]
	ValueIO       telemetry.Instrument[int64]
	KeySize       telemetry.Instrument[int64]
	ValueSize     telemetry.Instrument[int64]
}

func (ks *instrumentedKeyspace) Name() string {
	return ks.Next.Name()
}

func (ks *instrumentedKeyspace) Get(ctx context.Context, k []byte) ([]byte, Revision, error) {
	keySize := int64(len(k))

	ctx, span := ks.Telemetry.StartSpan(
		ctx,
		"keyspace.get",
		telemetry.Binary("key", k),
		telemetry.Int("key_size", keySize),
	)
	defer span.End()

	ks.KeyIO(ctx, keySize, telemetry.WriteDirection)
	ks.KeySize(ctx, keySize, telemetry.WriteDirection)

	v, r, err := ks.Next.Get(ctx, k)
	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.get.error", "unable to fetch value associated with key", err)
		return nil, 0, err
	}

	valueSize := int64(len(v))

	if valueSize != 0 {
		ks.ValueIO(ctx, valueSize, telemetry.ReadDirection)
		ks.ValueSize(ctx, valueSize, telemetry.ReadDirection)

		span.SetAttributes(
			telemetry.Bool("key_present", true),
			telemetry.Binary("value", v),
			telemetry.Int("value_size", valueSize),
			telemetry.Int("revision", r),
		)

		ks.Telemetry.Info(ctx, "keyspace.get.ok", "fetched value associated with key")
	} else {
		ks.Misses(ctx, 1)

		span.SetAttributes(
			telemetry.Bool("key_present", false),
		)

		ks.Telemetry.Info(ctx, "keyspace.get.ok", "key is not present in keyspace")
	}

	return v, r, nil
}

func (ks *instrumentedKeyspace) Has(ctx context.Context, k []byte) (bool, error) {
	keySize := int64(len(k))

	ctx, span := ks.Telemetry.StartSpan(
		ctx,
		"keyspace.has",
		telemetry.Binary("key", k),
		telemetry.Int("key_size", keySize),
	)
	defer span.End()

	ks.KeyIO(ctx, keySize, telemetry.WriteDirection)
	ks.KeySize(ctx, keySize, telemetry.WriteDirection)

	ok, err := ks.Next.Has(ctx, k)
	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.has.error", "unable to check presence of key in keyspace", err)
		return false, err
	}

	span.SetAttributes(
		telemetry.Bool("key_present", ok),
	)

	if ok {
		ks.Telemetry.Info(ctx, "keyspace.has.ok", "key is present in keyspace")
	} else {
		ks.Telemetry.Info(ctx, "keyspace.has.ok", "key is not present in keyspace")
	}

	return ok, nil
}

func (ks *instrumentedKeyspace) Set(ctx context.Context, k, v []byte, r Revision) error {
	keySize := int64(len(k))
	valueSize := int64(len(v))

	op := "keyspace.set"
	if valueSize == 0 {
		op = "keyspace.set.delete"
	}

	ctx, span := ks.Telemetry.StartSpan(
		ctx,
		op,
		telemetry.Binary("key", k),
		telemetry.Int("key_size", keySize),
		telemetry.Int("revision", r),
	)
	defer span.End()

	ks.KeyIO(ctx, keySize, telemetry.WriteDirection)
	ks.KeySize(ctx, keySize, telemetry.WriteDirection)

	if valueSize != 0 {
		span.SetAttributes(
			telemetry.Binary("value", v),
			telemetry.Int("value_size", valueSize),
		)

		ks.ValueIO(ctx, valueSize, telemetry.WriteDirection)
		ks.ValueSize(ctx, valueSize, telemetry.WriteDirection)
	}

	if err := ks.Next.Set(ctx, k, v, r); err != nil {
		if IsConflict(err) {
			ks.Telemetry.Error(ctx, "keyspace.set.conflict", "optimistic concurrency conflict", err)
			ks.Conflicts(ctx, 1)
			span.SetAttributes(telemetry.Bool("conflict", true))
		} else if valueSize == 0 {
			ks.Telemetry.Error(ctx, "keyspace.set.error", "unable to delete key/value pair", err)
		} else {
			ks.Telemetry.Error(ctx, "keyspace.set.error", "unable to set key/value pair", err)
		}

		return err
	}

	if valueSize == 0 {
		ks.Telemetry.Info(ctx, "keyspace.set.ok", "deleted key/value pair")
	} else {
		ks.Telemetry.Info(ctx, "keyspace.set.ok", "set key/value pair")
	}

	return nil
}

func (ks *instrumentedKeyspace) SetUnconditional(ctx context.Context, k, v []byte) error {
	keySize := int64(len(k))
	valueSize := int64(len(v))

	op := "keyspace.set-unconditional"
	if valueSize == 0 {
		op = "keyspace.set-unconditional.delete"
	}

	ctx, span := ks.Telemetry.StartSpan(
		ctx,
		op,
		telemetry.Binary("key", k),
		telemetry.Int("key_size", keySize),
	)
	defer span.End()

	ks.KeyIO(ctx, keySize, telemetry.WriteDirection)
	ks.KeySize(ctx, keySize, telemetry.WriteDirection)

	if valueSize != 0 {
		span.SetAttributes(
			telemetry.Binary("value", v),
			telemetry.Int("value_size", valueSize),
		)

		ks.ValueIO(ctx, valueSize, telemetry.WriteDirection)
		ks.ValueSize(ctx, valueSize, telemetry.WriteDirection)
	}

	if err := ks.Next.SetUnconditional(ctx, k, v); err != nil {
		if valueSize == 0 {
			ks.Telemetry.Error(ctx, "keyspace.set-unconditional.error", "unable to delete key/value pair", err)
		} else {
			ks.Telemetry.Error(ctx, "keyspace.set-unconditional.error", "unable to set key/value pair", err)
		}

		return err
	}

	if valueSize == 0 {
		ks.Telemetry.Info(ctx, "keyspace.set-unconditional.ok", "deleted key/value pair")
	} else {
		ks.Telemetry.Info(ctx, "keyspace.set-unconditional.ok", "set key/value pair")
	}

	return nil
}

func (ks *instrumentedKeyspace) Range(ctx context.Context, fn BinaryRangeFunc) error {
	ctx, span := ks.Telemetry.StartSpan(ctx, "keyspace.range")
	defer span.End()

	var (
		count     uint64
		totalSize int64
		brokeLoop bool
	)

	ks.Telemetry.Info(ctx, "keyspace.range.start", "reading key/value pairs")

	err := ks.Next.Range(
		ctx,
		func(ctx context.Context, k, v []byte, r Revision) (bool, error) {
			count++

			keySize := int64(len(k))
			valueSize := int64(len(v))
			totalSize += keySize + valueSize

			ks.KeyIO(ctx, keySize, telemetry.ReadDirection)
			ks.KeySize(ctx, keySize, telemetry.ReadDirection)

			ks.ValueIO(ctx, valueSize, telemetry.ReadDirection)
			ks.ValueSize(ctx, valueSize, telemetry.ReadDirection)

			ok, err := fn(ctx, k, v, r)
			if ok || err != nil {
				return ok, err
			}

			brokeLoop = true
			return false, nil
		},
	)

	span.SetAttributes(
		telemetry.Int("pairs_read", count),
		telemetry.Int("bytes_read", totalSize),
		telemetry.Bool("reached_end", !brokeLoop && err == nil),
	)

	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.range.error", "unable to range over key/value pairs", err)
		return err
	}

	if brokeLoop {
		ks.Telemetry.Info(ctx, "keyspace.range.break", "range aborted cleanly before visiting all key/value pairs")
	} else {
		ks.Telemetry.Info(ctx, "keyspace.range.end", "range visited all key/value pairs")
	}

	return nil
}

func (ks *instrumentedKeyspace) Close() error {
	if ks.Next == nil {
		// Closing an already-closed resource is not an error, allowing Close()
		// to be called unconditionally by a defer statement.
		return nil
	}

	ctx, span := ks.Telemetry.StartSpan(context.Background(), "keyspace.close")
	defer span.End()

	defer func() {
		ks.Next = nil
		ks.OpenKeyspaces(ctx, -1)
	}()

	if err := ks.Next.Close(); err != nil {
		ks.Telemetry.Error(ctx, "keyspace.close.error", "unable to close keyspace cleanly", err)
		return err
	}

	ks.Telemetry.Info(ctx, "keyspace.close.ok", "keyspace closed")

	return nil
}
