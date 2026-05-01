package dynamoset

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xdynamodb"
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

// Provision creates the DynamoDB table used by the store if it does not already
// exist.
//
// The store also creates the table on first use if it does not exist. Provision
// allows infrastructure to be created ahead of time, for example as part of a
// deployment pipeline, so that the application itself does not need broad IAM
// permissions.
func (s *store) Provision(ctx context.Context) error {
	return s.provisionOnce.Do(ctx, func(ctx context.Context) error {
		_, err := xdynamodb.CreateTableIfNotExists(
			ctx,
			s.Client,
			s.Table,
			s.OnRequest,
			xdynamodb.KeyAttr{
				Name:    &setAttr,
				Type:    types.ScalarAttributeTypeS,
				KeyType: types.KeyTypeHash,
			},
			xdynamodb.KeyAttr{
				Name:    &memberAttr,
				Type:    types.ScalarAttributeTypeB,
				KeyType: types.KeyTypeRange,
			},
		)
		return err
	})
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
