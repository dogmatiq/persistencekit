package dynamoset

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/set"
)

// BinaryStore is an implementation of [set.BinaryStore] that persists to a DynamoDB
// table.
type BinaryStore struct {
	client        *dynamodb.Client
	table         string
	onRequest     func(any) []func(*dynamodb.Options)
	provisionOnce xsync.SucceedOnce
}

// NewBinaryStore returns a new [set.BinaryStore] that uses the given DynamoDB
// client to store set members in the given table.
func NewBinaryStore(
	client *dynamodb.Client,
	table string,
	options ...Option,
) *BinaryStore {
	if table == "" {
		panic("table name must not be empty")
	}

	s := &BinaryStore{
		client: client,
		table:  table,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// Option is a functional option that changes the behavior of [NewBinaryStore].
type Option func(*BinaryStore)

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
	return func(s *BinaryStore) {
		s.onRequest = fn
	}
}

// Provision creates the DynamoDB table used by the store if it does not already
// exist.
//
// The store also creates the table on first use if it does not exist. Provision
// allows infrastructure to be created ahead of time, for example as part of a
// deployment pipeline, so that the application itself does not need broad IAM
// permissions.
func (s *BinaryStore) Provision(ctx context.Context) error {
	return s.provisionOnce.Do(ctx, func(ctx context.Context) error {
		_, err := dynamox.CreateTableIfNotExists(
			ctx,
			s.client,
			s.table,
			s.onRequest,
			dynamox.KeyAttr{
				Name:    &setAttr,
				Type:    types.ScalarAttributeTypeS,
				KeyType: types.KeyTypeHash,
			},
			dynamox.KeyAttr{
				Name:    &memberAttr,
				Type:    types.ScalarAttributeTypeB,
				KeyType: types.KeyTypeRange,
			},
		)
		return err
	})
}

// Open returns the set with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (set.BinarySet, error) {
	if err := s.Provision(ctx); err != nil {
		return nil, err
	}

	set := &setimpl{
		Client:    s.client,
		OnRequest: s.onRequest,
	}

	set.attr.Set.Value = name
	set.prepareRequests(s.table)

	return set, nil
}
