package dynamokv

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/internal/syncx"
	"github.com/dogmatiq/persistencekit/kv"
)

// BinaryStore is an implementation of [kv.BinaryStore] that persists to a
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

	create syncx.SucceedOnce
}

const (
	keyspaceAttr = "Keyspace"
	keyAttr      = "Key"
	valueAttr    = "Value"
)

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (kv.BinaryKeyspace, error) {
	if s.Table == "" {
		panic("table name must not be empty")
	}

	if err := s.createTable(ctx); err != nil {
		return nil, err
	}

	ks := &keyspace{
		Client:    s.Client,
		OnRequest: s.OnRequest,

		name:  &types.AttributeValueMemberS{Value: name},
		key:   &types.AttributeValueMemberB{},
		value: &types.AttributeValueMemberB{},
	}

	ks.getRequest = dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			keyspaceAttr: ks.name,
			keyAttr:      ks.key,
		},
		ProjectionExpression: aws.String(`#V`),
		ExpressionAttributeNames: map[string]string{
			"#V": valueAttr,
		},
	}

	// Has() requests an unknown attribute to avoid fetching unnecessary data.
	ks.hasRequest = dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			keyspaceAttr: ks.name,
			keyAttr:      ks.key,
		},
		ProjectionExpression: aws.String(`NonExistent`),
	}

	ks.queryRequest = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#S = :S`),
		ProjectionExpression:   aws.String("#K, #V"),
		ExpressionAttributeNames: map[string]string{
			"#S": keyspaceAttr,
			"#K": keyAttr,
			"#V": valueAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":S": ks.name,
		},
	}

	ks.putRequest = dynamodb.PutItemInput{
		TableName: aws.String(s.Table),
		Item: map[string]types.AttributeValue{
			keyspaceAttr: ks.name,
			keyAttr:      ks.key,
			valueAttr:    ks.value,
		},
	}

	ks.deleteRequest = dynamodb.DeleteItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			keyspaceAttr: ks.name,
			keyAttr:      ks.key,
		},
	}

	return ks, nil
}

func (s *BinaryStore) createTable(ctx context.Context) error {
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
							AttributeName: aws.String(keyspaceAttr),
							AttributeType: types.ScalarAttributeTypeS,
						},
						{
							AttributeName: aws.String(keyAttr),
							AttributeType: types.ScalarAttributeTypeB,
						},
					},
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String(keyspaceAttr),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String(keyAttr),
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
