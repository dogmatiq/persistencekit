package dynamojournal

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/internal/syncx"
	"github.com/dogmatiq/persistencekit/journal"
)

// store is an implementation of [journal.BinaryStore] that persists to a
// DynamoDB table.
type store struct {
	Client    *dynamodb.Client
	Table     string
	OnRequest func(any) []func(*dynamodb.Options)

	create syncx.SucceedOnce
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

const (
	nameAttr      = "Name"
	positionAttr  = "Position"
	recordAttr    = "Record"
	truncatedAttr = "Truncated"
)

// Open returns the journal with the given name.
func (s *store) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	if s.Table == "" {
		panic("table name must not be empty")
	}

	if err := s.createTable(ctx); err != nil {
		return nil, err
	}

	j := &journ{
		Client:    s.Client,
		OnRequest: s.OnRequest,

		name:     &types.AttributeValueMemberS{Value: name},
		position: &types.AttributeValueMemberN{},
		record:   &types.AttributeValueMemberB{},
	}

	j.boundsReq = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#N = :N`),
		ProjectionExpression:   aws.String("#P, #T"),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
			"#P": positionAttr,
			"#T": truncatedAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":N": j.name,
		},
		ScanIndexForward: aws.Bool(true),
		Limit:            aws.Int32(1),
	}

	j.getReq = dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
		ProjectionExpression: aws.String(`#R, #T`),
		ExpressionAttributeNames: map[string]string{
			"#R": recordAttr,
			"#T": truncatedAttr,
		},
	}

	j.rangeReq = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#N = :N AND #P >= :P`),
		FilterExpression:       aws.String(`attribute_not_exists(#T)`),
		ProjectionExpression:   aws.String("#P, #R"),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
			"#P": positionAttr,
			"#R": recordAttr,
			"#T": truncatedAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":N": j.name,
			":P": j.position,
		},
	}

	j.appendReq = dynamodb.PutItemInput{
		TableName:           aws.String(s.Table),
		ConditionExpression: aws.String(`attribute_not_exists(#N)`),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
		},
		Item: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
			recordAttr:   j.record,
		},
	}

	j.truncateReq = dynamodb.UpdateItemInput{
		TableName:           aws.String(s.Table),
		ConditionExpression: aws.String(`attribute_not_exists(#T)`),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
		ExpressionAttributeNames: map[string]string{
			"#T": truncatedAttr,
			"#R": recordAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":T": &types.AttributeValueMemberBOOL{Value: true},
		},
		UpdateExpression: aws.String(`SET #T = :T REMOVE #R`),
	}

	j.deleteReq = dynamodb.DeleteItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
	}

	return j, nil
}

func (s *store) createTable(ctx context.Context) error {
	return s.create.Do(
		func() error {
			if _, err := awsx.Do(
				ctx,
				s.Client.CreateTable,
				s.OnRequest,
				&dynamodb.CreateTableInput{
					TableName: aws.String(s.Table),
					AttributeDefinitions: []types.AttributeDefinition{
						{
							AttributeName: aws.String(nameAttr),
							AttributeType: types.ScalarAttributeTypeS,
						},
						{
							AttributeName: aws.String(positionAttr),
							AttributeType: types.ScalarAttributeTypeN,
						},
					},
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String(nameAttr),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String(positionAttr),
							KeyType:       types.KeyTypeRange,
						},
					},
					BillingMode: types.BillingModePayPerRequest,
				},
			); err != nil && !errors.As(err, new(*types.ResourceInUseException)) {
				return fmt.Errorf("unable to create DynamoDB table: %w", err)
			}
			return nil
		},
	)
}
