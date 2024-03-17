package dynamokv

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/kv"
)

// BinaryStore is an implementation of [kv.BinaryStore] that persists to a
// DynamoDB table.
type BinaryStore struct {
	// Client is the DynamoDB client to use.
	Client *dynamodb.Client

	// Table is the table name used for storage of journal records.
	Table string

	// DecorateGetItem is an optional function that is called before each
	// DynamoDB "GetItem" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecorateGetItem func(*dynamodb.GetItemInput) []func(*dynamodb.Options)

	// DecorateQuery is an optional function that is called before each DynamoDB
	// "Query" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecorateQuery func(*dynamodb.QueryInput) []func(*dynamodb.Options)

	// DecoratePutItem is an optional function that is called before each
	// DynamoDB "PutItem" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecoratePutItem func(*dynamodb.PutItemInput) []func(*dynamodb.Options)

	// DecorateDeleteItem is an optional function that is called before each
	// DynamoDB "DeleteItem" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecorateDeleteItem func(*dynamodb.DeleteItemInput) []func(*dynamodb.Options)
}

// Open returns the keyspace with the given name.
func (s *BinaryStore) Open(_ context.Context, name string) (kv.BinaryKeyspace, error) {
	ks := &keyspace{
		Client:             s.Client,
		DecorateGetItem:    s.DecorateGetItem,
		DecorateQuery:      s.DecorateQuery,
		DecoratePutItem:    s.DecoratePutItem,
		DecorateDeleteItem: s.DecorateDeleteItem,

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
