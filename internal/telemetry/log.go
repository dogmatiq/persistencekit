package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// Info logs an informational message to the log and as a span event.
func (r *Recorder) Info(
	ctx context.Context,
	event, message string,
	body ...Attr,
) {
	r.log(ctx, log.SeverityInfo, event, message, nil, body)
}

// Error logs an error message to the log and as a span event.
//
// It marks the span as an error and emits increments the "errors" metric.
func (r *Recorder) Error(
	ctx context.Context,
	event string,
	err error,
	body ...Attr,
) {
	r.log(ctx, log.SeverityError, event, err.Error(), err, body)
	r.errorCount(ctx, 1)

	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Error, err.Error())
	span.RecordError(err)
}

func (r *Recorder) log(
	ctx context.Context,
	severity log.Severity,
	event, message string,
	err error,
	body []Attr,
) {
	if !r.logger.Enabled(
		ctx,
		log.EnabledParameters{
			Severity: severity,
		},
	) {
		return
	}

	span := trace.SpanFromContext(ctx)
	span.AddEvent(
		event,
		trace.WithAttributes(attribute.String("message", message)),
		trace.WithAttributes(asAttrKeyValues(body)...),
	)

	var rec log.Record
	rec.SetEventName(event)
	rec.SetSeverity(severity)
	rec.AddAttributes(log.String("message", message))

	if err != nil {
		rec.AddAttributes(log.String("error", err.Error()))
	}

	if len(body) != 0 {
		kvs := asLogKeyValues(body)
		rec.SetBody(log.MapValue(kvs...))
	}

	r.logger.Emit(ctx, rec)
}
