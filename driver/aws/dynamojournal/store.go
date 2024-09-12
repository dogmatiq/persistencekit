package dynamojournal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/internal/syncx"
	"github.com/dogmatiq/persistencekit/journal"
)

// store is an implementation of [journal.BinaryStore] that persists to a
// DynamoDB table.
type store struct {
	Client    *dynamodb.Client
	Table     string
	OnRequest func(any) []func(*dynamodb.Options)

	createOnce syncx.SucceedOnce
}

// NewBinaryStore returns a new [journal.BinaryStore] that uses the given
// DynamoDB client to store journal records in the given table.
func NewBinaryStore(
	client *dynamodb.Client,
	table string,
	options ...Option,
) journal.BinaryStore {
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

// Open returns the journal with the given name.
func (s *store) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	if s.Table == "" {
		panic("table name must not be empty")
	}

	if err := s.createOnce.Do(ctx, s.createTable); err != nil {
		return nil, err
	}

	j := &journ{
		Client:    s.Client,
		OnRequest: s.OnRequest,
	}

	if err := j.init(ctx, s.Table, name); err != nil {
		return nil, err
	}

	return j, nil
}
