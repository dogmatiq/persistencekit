package telemetry

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"golang.org/x/exp/constraints"
)

// Attr is a telemetry attribute.
type Attr struct {
	typ attrType
	key string
	str string
	num uint64
}

// String returns a string attribute.
func String[T ~string](k string, v T) Attr {
	return Attr{
		typ: attrTypeString,
		key: k,
		str: string(v),
	}
}

// Stringer returns a string attribute. The value is the result of calling
// v.String().
func Stringer(k string, v fmt.Stringer) Attr {
	return String(k, v.String())
}

// Type returns a string attribute set to the name of T.
func Type[T any](k string, v T) Attr {
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return String(k, t.String())
}

// Bool returns a boolean attribute.
func Bool[T ~bool](k string, v T) Attr {
	var n uint64
	if v {
		n = 1
	}

	return Attr{
		typ: attrTypeBool,
		key: k,
		num: n,
	}
}

// Int returns an int64 attribute.
func Int[T constraints.Integer](k string, v T) Attr {
	return Attr{
		typ: attrTypeInt64,
		key: k,
		num: uint64(v),
	}
}

// If conditionally includes an attribute.
func If(cond bool, attr Attr) Attr {
	if cond {
		return attr
	}
	return Attr{}
}

// Float returns a float64 attribute.
func Float[T constraints.Float](k string, v T) Attr {
	return Attr{
		typ: attrTypeFloat64,
		key: k,
		num: math.Float64bits(float64(v)),
	}
}

// Binary returns a string attribute containing v, represented as a Go string
// (with backslash escaped sequences). If the value is longer than 64 bytes, it
// is truncated to 64 bytes and the key is suffixed with "_truncated".
func Binary(k string, v []byte) Attr {
	if len(v) > 64 {
		v = v[:64]
		k += "_truncated"
	}

	return Attr{
		key: k,
		str: strconv.QuoteToASCII(string(v)),
	}
}

// isShortASCII returns true if k is a non-empty ASCII string short enough that
// it may be included as a telemetry attribute.
func isShortASCII(k []byte) bool {
	if len(k) == 0 || len(k) > 128 {
		return false
	}

	for _, octet := range k {
		if octet < ' ' || octet > '~' {
			return false
		}
	}

	return true
}

func (a Attr) asAttrKeyValue() (attribute.KeyValue, bool) {
	switch a.typ {
	case attrTypeNone:
		return attribute.KeyValue{}, false
	case attrTypeString:
		return attribute.String(a.key, a.str), true
	case attrTypeBool:
		return attribute.Bool(a.key, a.num != 0), true
	case attrTypeInt64:
		return attribute.Int64(a.key, int64(a.num)), true
	case attrTypeFloat64:
		return attribute.Float64(a.key, math.Float64frombits(a.num)), true
	default:
		panic("unknown attribute type")
	}
}

func (a Attr) asLogKeyValue() (log.KeyValue, bool) {
	switch a.typ {
	case attrTypeNone:
		return log.KeyValue{}, false
	case attrTypeString:
		return log.String(a.key, a.str), true
	case attrTypeBool:
		return log.Bool(a.key, a.num != 0), true
	case attrTypeInt64:
		return log.Int64(a.key, int64(a.num)), true
	case attrTypeFloat64:
		return log.Float64(a.key, math.Float64frombits(a.num)), true
	default:
		panic("unknown attribute type")
	}
}

type attrType uint8

const (
	attrTypeNone attrType = iota
	attrTypeString
	attrTypeBool
	attrTypeInt64
	attrTypeFloat64
)

func asAttrKeyValues(attrs []Attr) []attribute.KeyValue {
	kvs := make([]attribute.KeyValue, 0, len(attrs))

	for _, attr := range attrs {
		if attr, ok := attr.asAttrKeyValue(); ok {
			kvs = append(kvs, attr)
		}
	}

	return kvs
}

func asLogKeyValues(attrs []Attr) []log.KeyValue {
	kvs := make([]log.KeyValue, 0, len(attrs))

	for _, attr := range attrs {
		if attr, ok := attr.asLogKeyValue(); ok {
			kvs = append(kvs, attr)
		}
	}

	return kvs
}

// func (s *Span) resolveAttrs(attrs []Attr) ([]attribute.KeyValue, []any) {
// 	tel := make([]attribute.KeyValue, 0, len(attrs))
// 	log := make([]any, 0, len(attrs))

// 	prefix := attribute.Key(s.recorder.name + ".")

// 	for _, attr := range attrs {
// 		if attr, ok := attr.otel(); ok {
// 			attr.Key = prefix + attr.Key
// 			tel = append(tel, attr)
// 		}
// 		if attr, ok := attr.slog(); ok {
// 			log = append(log, attr)
// 		}
// 	}

// 	return tel, log
// }

// var errorsStringType = reflect.TypeOf(errors.New(""))

// // isStringError returns true if err is an error that was created using
// // errors.New() or fmt.Errorf(), and therefore has no meaningful type.
// func isStringError(err error) bool {
// 	return reflect.TypeOf(err) == errorsStringType
// }

// // unwrapError unwraps err until an error with a meaningful type is found. If
// // all errors in the chain are "string errors", it returns nil.
// func unwrapError(err error) error {
// 	for err != nil && isStringError(err) {
// 		err = errors.Unwrap(err)
// 	}
// 	return err
// }
