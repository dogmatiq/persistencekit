package dynamox

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
)

// RangeFunc is a function that is called for each item in a result set.
type RangeFunc func(context.Context, map[string]types.AttributeValue) (bool, error)

// Range executes a query and calls fn for each item in the result set.
func Range(
	ctx context.Context,
	client *dynamodb.Client,
	m func(any) []func(*dynamodb.Options),
	in *dynamodb.QueryInput,
	fn RangeFunc,
) error {
	in.ExclusiveStartKey = nil

	for {
		out, err := awsx.Do(ctx, client.Query, m, in)
		if err != nil {
			return err
		}

		for _, item := range out.Items {
			if ok, err := fn(ctx, item); err != nil || !ok {
				return err
			}
		}

		if out.LastEvaluatedKey == nil {
			return nil
		}

		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}
