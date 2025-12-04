package dynamoset

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
)

var (
	// setAttr is the name of the attribute that stores the set name
	// on each item. Together with [memberAttr], it forms the primary key of the
	// table.
	setAttr = "S"

	// memberAttr is the name of the attribute that stores the set member on
	// each item.
	memberAttr = "M"

	// nonExistentAttr is the name of an attribute that does not exist on any
	// item. It is used to test for the existence of an item without fetching
	// unnecessary data.
	nonExistentAttr = "X"
)

// createTable creates the DynamoDB table if it does not already exist.
func (s *store) createTable(ctx context.Context) error {
	return dynamox.CreateTableIfNotExists(
		ctx,
		s.Client,
		s.Table,
		s.OnRequest,
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
}

func (s *setimpl) prepareRequests(table string) {
	key := map[string]types.AttributeValue{
		setAttr:    &s.attr.Set,
		memberAttr: &s.attr.Member,
	}

	// Has requests [nonExistentAttr] for the item at s.attr.Member to check if
	// the item exists at all.
	s.request.Has = dynamodb.GetItemInput{
		TableName:            &table,
		Key:                  key,
		ProjectionExpression: &nonExistentAttr,
	}

	// Range fetches all members of the set.
	s.request.Range = dynamodb.QueryInput{
		TableName:              &table,
		KeyConditionExpression: aws.String(`#S = :S`),
		ProjectionExpression:   aws.String("#M"),
		ExpressionAttributeNames: map[string]string{
			"#S": setAttr,
			"#M": memberAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":S": &s.attr.Set,
		},
	}

	// Add adds s.attr.Member to the set.
	s.request.Put = dynamodb.PutItemInput{
		TableName: &table,
		Item:      key,
	}

	// Delete removes s.attr.Member from the set.
	s.request.Delete = dynamodb.DeleteItemInput{
		TableName: &table,
		Key:       key,
	}
}
