package kv

import (
	"context"

	"github.com/dogmatiq/persistencekit/internal/telemetry"
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
		telemetry.Type("store", s.Next),
		telemetry.String("handle", telemetry.HandleID()),
		telemetry.String("name", name),
	)

	ks := &instrumentedKeyspace{
		Telemetry:  telem,
		OpenCount:  telem.UpDownCounter("open_keyspaces", "{keyspace}", "The number of keyspaces that are currently open."),
		MissCount:  telem.Counter("misses", "{operation}", "The number of times the value associated with a specific key was requested but not present in the keyspace."),
		KeyCount:   telem.Counter("keys", "{key}", "The number of keys that have been operated upon."),
		KeyBytes:   telem.Counter("key_bytes", "By", "The cumulative size of the keys that have been operated upon."),
		KeySizes:   telem.Histogram("key_sizes", "By", "The sizes of the keys that have been operated upon."),
		ValueCount: telem.Counter("values", "{value}", "The number of values that have been operated upon."),
		ValueBytes: telem.Counter("value_bytes", "By", "The cumulative size of the values that have been operated upon."),
		ValueSizes: telem.Histogram("value_sizes", "By", "The sizes of the values that have been operated upon."),
	}

	ctx, span := telem.StartSpan(ctx, "keyspace.open")
	defer span.End()

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.open.error", err)
		return nil, err
	}

	ks.Next = next

	ks.OpenCount(ctx, 1)
	ks.Telemetry.Info(ctx, "keyspace.open.ok", "opened keyspace")

	return ks, nil
}

type instrumentedKeyspace struct {
	Next      BinaryKeyspace
	Telemetry *telemetry.Recorder

	OpenCount  telemetry.Instrument[int64]
	MissCount  telemetry.Instrument[int64]
	KeyCount   telemetry.Instrument[int64]
	KeyBytes   telemetry.Instrument[int64]
	KeySizes   telemetry.Instrument[int64]
	ValueCount telemetry.Instrument[int64]
	ValueBytes telemetry.Instrument[int64]
	ValueSizes telemetry.Instrument[int64]
}

func (ks *instrumentedKeyspace) Name() string {
	return ks.Next.Name()
}

func (ks *instrumentedKeyspace) Get(ctx context.Context, k []byte) ([]byte, error) {
	keySize := int64(len(k))

	ctx, span := ks.Telemetry.StartSpan(
		ctx,
		"keyspace.get",
		telemetry.Binary("key", k),
		telemetry.Int("key_size", keySize),
	)
	defer span.End()

	ks.KeyCount(ctx, 1, telemetry.WriteDirection)
	ks.KeyBytes(ctx, keySize, telemetry.WriteDirection)
	ks.KeySizes(ctx, keySize, telemetry.WriteDirection)

	v, err := ks.Next.Get(ctx, k)
	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.get.error", err)
		return nil, err
	}

	valueSize := int64(len(v))

	if valueSize != 0 {
		ks.ValueCount(ctx, 1, telemetry.ReadDirection)
		ks.ValueBytes(ctx, valueSize, telemetry.ReadDirection)
		ks.ValueSizes(ctx, valueSize, telemetry.ReadDirection)

		span.SetAttributes(
			telemetry.Bool("key_present", true),
			telemetry.Binary("value", v),
			telemetry.Int("value_size", valueSize),
		)

		ks.Telemetry.Info(ctx, "keyspace.get.ok", "fetched value associated with key")
	} else {
		ks.MissCount(ctx, 1)

		span.SetAttributes(
			telemetry.Bool("key_present", false),
		)

		ks.Telemetry.Info(ctx, "keyspace.get.ok", "key is not present in keyspace")
	}

	return v, nil
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

	ks.KeyCount(ctx, 1, telemetry.WriteDirection)
	ks.KeyBytes(ctx, keySize, telemetry.WriteDirection)
	ks.KeySizes(ctx, keySize, telemetry.WriteDirection)

	ok, err := ks.Next.Has(ctx, k)
	if err != nil {
		ks.Telemetry.Error(ctx, "keyspace.has.error", err)
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

func (ks *instrumentedKeyspace) Set(ctx context.Context, k, v []byte) error {
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
	)
	defer span.End()

	ks.KeyCount(ctx, 1, telemetry.WriteDirection)
	ks.KeyBytes(ctx, keySize, telemetry.WriteDirection)
	ks.KeySizes(ctx, keySize, telemetry.WriteDirection)

	if valueSize != 0 {
		span.SetAttributes(
			telemetry.Binary("value", v),
			telemetry.Int("value_size", valueSize),
		)

		ks.ValueCount(ctx, 1, telemetry.WriteDirection)
		ks.ValueBytes(ctx, valueSize, telemetry.WriteDirection)
		ks.ValueSizes(ctx, valueSize, telemetry.WriteDirection)
	}

	if err := ks.Next.Set(ctx, k, v); err != nil {
		ks.Telemetry.Error(ctx, "keyspace.set.error", err)
		return err
	}

	if valueSize == 0 {
		ks.Telemetry.Info(ctx, "keyspace.set.ok", "deleted key/value pair")
	} else {
		ks.Telemetry.Info(ctx, "keyspace.set.ok", "set key/value pair")
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
		func(ctx context.Context, k, v []byte) (bool, error) {
			count++

			keySize := int64(len(k))
			valueSize := int64(len(v))
			totalSize += keySize + valueSize

			ks.KeyCount(ctx, 1, telemetry.ReadDirection)
			ks.KeyBytes(ctx, keySize, telemetry.ReadDirection)
			ks.KeySizes(ctx, keySize, telemetry.ReadDirection)

			ks.ValueCount(ctx, 1, telemetry.ReadDirection)
			ks.ValueBytes(ctx, valueSize, telemetry.ReadDirection)
			ks.ValueSizes(ctx, valueSize, telemetry.ReadDirection)

			ok, err := fn(ctx, k, v)
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
		ks.Telemetry.Error(ctx, "keyspace.range.error", err)
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
		ks.OpenCount(ctx, -1)
	}()

	if err := ks.Next.Close(); err != nil {
		ks.Telemetry.Error(ctx, "keyspace.close.error", err)
		return err
	}

	ks.Telemetry.Info(ctx, "keyspace.close.ok", "keyspace closed")

	return nil
}
