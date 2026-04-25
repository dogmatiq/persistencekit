package dynamojournal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/journal"
)

// store is an implementation of [journal.BinaryStore] that persists to a
// DynamoDB table.
type store struct {
	Client    *dynamodb.Client
	Table     string
	OnRequest func(any) []func(*dynamodb.Options)

	provisionOnce xsync.SucceedOnce
}

// NewBinaryStore returns a new [journal.BinaryStore] that uses the given
// DynamoDB client to store journal records in the given table.
func NewBinaryStore(
	client *dynamodb.Client,
	table string,
	options ...Option,
) journal.BinaryStore {
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

// Provision creates the DynamoDB table used by the store if it does not already
// exist.
//
// The store also creates the table on first use if it does not exist. Provision
// allows infrastructure to be created ahead of time, for example as part of a
// deployment pipeline, so that the application itself does not need broad IAM
// permissions.
func (s *store) Provision(ctx context.Context) error {
	return s.provisionOnce.Do(ctx, func(ctx context.Context) error {
		_, err := dynamox.CreateTableIfNotExists(
			ctx,
			s.Client,
			s.Table,
			s.OnRequest,
			dynamox.KeyAttr{
				Name:    &journalAttr,
				Type:    types.ScalarAttributeTypeS,
				KeyType: types.KeyTypeHash,
			},
			dynamox.KeyAttr{
				Name:    &positionAttr,
				Type:    types.ScalarAttributeTypeN,
				KeyType: types.KeyTypeRange,
			},
		)
		return err
	})
}

// Open returns the journal with the given name.
func (s *store) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	if err := s.Provision(ctx); err != nil {
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
