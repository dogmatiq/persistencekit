package journal

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
	t trace.TracerProvider,
	m metric.MeterProvider,
	l log.LoggerProvider,
) BinaryStore {
	return &instrumentedStore{
		Next: s,
		Telemetry: telemetry.Provider{
			TracerProvider: t,
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

// Open returns the journal with the given name.
func (s *instrumentedStore) Open(ctx context.Context, name string) (BinaryJournal, error) {
	telem := s.Telemetry.Recorder(
		"github.com/dogmatiq/persistencekit/journal",
		telemetry.Type("journal.store", s.Next),
		telemetry.String("journal.name", name),
		telemetry.String("journal.handle", xtelemetry.HandleID()),
	)

	j := &instrumentedJournal{
		Telemetry:    telem,
		OpenJournals: telem.UpDownCounter("open_journals", "{journal}", "The number of journal handles that are currently open."),
		Conflicts:    telem.Counter("conflicts", "{error}", "The number of times appending a record to the journal has failed due to an optimistic-concurrency conflict."),
		RecordIO:     telem.Counter("record.io", "By", "The cumulative size of the journal records that have been operated upon."),
		RecordSize:   telem.Histogram("record.size", "By", "The sizes of the journal records that have been operated upon."),
	}

	ctx, span := j.Telemetry.StartSpan(ctx, "journal.open")
	defer span.End()

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		j.Telemetry.Error(ctx, "journal.open.error", "unable to open journal", err)
		return nil, err
	}

	j.Next = next

	j.OpenJournals(ctx, 1)
	j.Telemetry.Info(ctx, "journal.open.ok", "journal opened")

	return j, nil
}

type instrumentedJournal struct {
	Next      BinaryJournal
	Telemetry *telemetry.Recorder

	OpenJournals telemetry.Instrument[int64]
	Conflicts    telemetry.Instrument[int64]
	RecordIO     telemetry.Instrument[int64]
	RecordSize   telemetry.Instrument[int64]
}

func (j *instrumentedJournal) Name() string {
	return j.Next.Name()
}

func (j *instrumentedJournal) Bounds(ctx context.Context) (bounds Interval, err error) {
	ctx, span := j.Telemetry.StartSpan(ctx, "journal.bounds")
	defer span.End()

	bounds, err = j.Next.Bounds(ctx)
	if err != nil {
		j.Telemetry.Error(ctx, "journal.bounds.error", "unable to fetch journal bounds", err)
		return Interval{}, err
	}

	span.SetAttributes(
		telemetry.Int("begin", bounds.Begin),
		telemetry.Int("end", bounds.End),
	)

	j.Telemetry.Info(ctx, "journal.bounds.ok", "fetched journal bounds")

	return bounds, nil
}

func (j *instrumentedJournal) Get(ctx context.Context, pos Position) ([]byte, error) {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.get",
		telemetry.Int("position", pos),
	)
	defer span.End()

	rec, err := j.Next.Get(ctx, pos)
	if err != nil {
		j.Telemetry.Error(ctx, "journal.get.error", "unable to fetch journal record", err)
		return nil, err
	}

	size := int64(len(rec))

	span.SetAttributes(
		telemetry.Binary("record", rec),
		telemetry.Int("record_size", size),
	)

	j.RecordIO(ctx, size, telemetry.ReadDirection)
	j.RecordSize(ctx, size, telemetry.ReadDirection)

	j.Telemetry.Info(ctx, "journal.get.ok", "fetched journal record")

	return rec, nil
}

func (j *instrumentedJournal) Range(
	ctx context.Context,
	begin Position,
	fn BinaryRangeFunc,
) error {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.range",
		telemetry.Int("range_start", begin),
	)
	defer span.End()

	var (
		first, count Position
		totalSize    int64
		brokeLoop    bool
	)

	j.Telemetry.Info(ctx, "journal.range.start", "reading journal records")

	err := j.Next.Range(
		ctx,
		begin,
		func(ctx context.Context, pos Position, rec []byte) (bool, error) {
			if count == 0 {
				first = pos
			}
			count++

			size := int64(len(rec))
			totalSize += size

			j.RecordIO(ctx, size, telemetry.ReadDirection)
			j.RecordSize(ctx, size, telemetry.ReadDirection)

			ok, err := fn(ctx, pos, rec)
			if ok || err != nil {
				return ok, err
			}

			brokeLoop = true
			return false, nil
		},
	)

	if count != 0 {
		span.SetAttributes(
			telemetry.Int("range_start", first),
			telemetry.Int("range_stop", first+count-1),
		)
	}

	span.SetAttributes(
		telemetry.Int("record_read", count),
		telemetry.Int("bytes_read", totalSize),
		telemetry.Bool("reached_end", !brokeLoop && err == nil),
	)

	if err != nil {
		j.Telemetry.Error(ctx, "journal.range.error", "unable to range over journal records", err)
		return err
	}

	if brokeLoop {
		j.Telemetry.Info(ctx, "journal.range.break", "range aborted cleanly before reaching the end of the journal")
	} else {
		j.Telemetry.Info(ctx, "journal.range.end", "range reached the end of the journal")
	}

	return nil
}

func (j *instrumentedJournal) Append(ctx context.Context, pos Position, rec []byte) error {
	size := int64(len(rec))

	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.append",
		telemetry.Int("position", pos),
		telemetry.Int("record_size", size),
	)
	defer span.End()

	j.RecordIO(ctx, size, telemetry.WriteDirection)
	j.RecordSize(ctx, size, telemetry.WriteDirection)

	err := j.Next.Append(ctx, pos, rec)
	if err != nil {
		if IsConflict(err) {
			j.Telemetry.Error(ctx, "journal.append.conflict", "optimistic concurrency conflict", err)
			j.Conflicts(ctx, 1)
			span.SetAttributes(telemetry.Bool("conflict", true))
		} else {
			j.Telemetry.Error(ctx, "journal.append.error", "unable to append journal record", err)
		}

		return err
	}

	j.Telemetry.Info(ctx, "journal.append.ok", "journal record appended")

	return nil
}

func (j *instrumentedJournal) Truncate(ctx context.Context, pos Position) error {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.truncate",
		telemetry.Int("position", pos),
	)
	defer span.End()

	if err := j.Next.Truncate(ctx, pos); err != nil {
		j.Telemetry.Error(ctx, "journal.truncate.error", "unable to truncate journal records", err)
		return err
	}

	j.Telemetry.Info(ctx, "journal.truncate.ok", "truncated oldest journal records")

	return nil
}

func (j *instrumentedJournal) Close() error {
	if j.Next == nil {
		// Closing an already-closed resource is not an error, allowing Close()
		// to be called unconditionally by a defer statement.
		return nil
	}

	ctx, span := j.Telemetry.StartSpan(context.Background(), "journal.close")
	defer span.End()

	defer func() {
		j.Next = nil
		j.OpenJournals(ctx, -1)
	}()

	if err := j.Next.Close(); err != nil {
		j.Telemetry.Error(ctx, "journal.close.error", "unable to close journal cleanly", err)
		return err
	}

	j.Telemetry.Info(ctx, "journal.close.ok", "journal closed")

	return nil
}
