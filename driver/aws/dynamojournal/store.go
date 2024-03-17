package dynamojournal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/journal"
)

// BinaryStore is an implementation of [journal.BinaryStore] that persists to a
// DynamoDB table.
type BinaryStore struct {
	// Client is the DynamoDB client to use.
	Client *dynamodb.Client

	// Table is the table name used for storage of journal records.
	Table string

	// OnRequest is a hook that is called before each DynamoDB request.
	//
	// It is passed a pointer to the input struct, e.g. [dynamodb.GetItemInput],
	// which it may modify in-place. It may be called with any DynamoDB request
	// type. The types of requests used may change in any version without
	// notice.
	//
	// Any functions returned by the function will be applied to the request's
	// options before the request is sent.
	OnRequest func(any) []func(*dynamodb.Options)

	created  atomic.Bool
	createdM sync.Mutex
}

const (
	nameAttr     = "Name"
	positionAttr = "Position"
	recordAttr   = "Record"
)

// Open returns the journal with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
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

	j.boundsQueryRequest = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#N = :N`),
		ProjectionExpression:   aws.String("#P"),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
			"#P": positionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":N": j.name,
		},
		ScanIndexForward: aws.Bool(true),
		Limit:            aws.Int32(1),
	}

	j.getRequest = dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
		ProjectionExpression: aws.String(`#R`),
		ExpressionAttributeNames: map[string]string{
			"#R": recordAttr,
		},
	}

	j.rangeQueryRequest = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#N = :N AND #P >= :P`),
		ProjectionExpression:   aws.String("#P, #R"),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
			"#P": positionAttr,
			"#R": recordAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":N": j.name,
			":P": j.position,
		},
	}

	j.putRequest = dynamodb.PutItemInput{
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

	j.deleteRequest = dynamodb.DeleteItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
	}

	return j, nil
}

func (s *BinaryStore) createTable(ctx context.Context) error {
	if s.created.Load() {
		return nil
	}

	s.createdM.Lock()
	defer s.createdM.Unlock()

	if s.created.Load() {
		return nil
	}

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

	s.created.Store(true)

	return nil
}
