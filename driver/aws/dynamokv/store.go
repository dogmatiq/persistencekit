package dynamokv

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/internal/syncx"
	"github.com/dogmatiq/persistencekit/kv"
)

// BinaryStore is an implementation of [kv.BinaryStore] that persists to a
// DynamoDB table.
type store struct {
	Client    *dynamodb.Client
	Table     string
	OnRequest func(any) []func(*dynamodb.Options)

	createTableOnce syncx.SucceedOnce
}

// NewBinaryStore returns a new [kv.BinaryStore] that uses the given DynamoDB
// client to store key/value pairs in the given table.
func NewBinaryStore(
	client *dynamodb.Client,
	table string,
	options ...Option,
) kv.BinaryStore {
	if table == "" {
		panic("table name must not be empty")
	}

	s := &store{
		Client: client,
		Table:  table,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// Option is a functional option that changes the behavior of [NewBinaryStore].
type Option func(*store)

// WithRequestHook is an [Option] that configures fn as a pre-request hook.
//
// Before each DynamoDB API request, fn is passed a pointer to the input struct,
// e.g. [dynamodb.GetItemInput], which it may modify in-place. It may be called
// with any DynamoDB request type. The types of requests used may change in any
// version without notice.
//
// Any functions returned by fn will be applied to the request's options before
// the request is sent.
func WithRequestHook(fn func(any) []func(*dynamodb.Options)) Option {
	return func(s *store) {
		s.OnRequest = fn
	}
}

// Open returns the keyspace with the given name.
func (s *store) Open(ctx context.Context, name string) (kv.BinaryKeyspace, error) {
	if err := s.createTableOnce.Do(ctx, s.createTable); err != nil {
		return nil, err
	}

	ks := &keyspace{
		Client:    s.Client,
		OnRequest: s.OnRequest,
	}

	ks.attr.Keyspace.Value = name
	ks.prepareRequests(s.Table)

	return ks, nil
}
