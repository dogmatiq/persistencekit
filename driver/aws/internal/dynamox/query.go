package dynamox

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
)

// QueryRange executes a query and calls fn for each item in the result set.
func QueryRange(
	ctx context.Context,
	client *dynamodb.Client,
	m func(any) []func(*dynamodb.Options),
	in *dynamodb.QueryInput,
	fn func(context.Context, map[string]types.AttributeValue) (bool, error),
) error {
	if in.Limit != nil && *in.Limit == 1 {
		panic("QueryRange() requires a query input with a limit greater than 1, or use QueryOne() instead")
	}
	return query(ctx, client, m, in, fn)
}

func query(
	ctx context.Context,
	client *dynamodb.Client,
	m func(any) []func(*dynamodb.Options),
	in *dynamodb.QueryInput,
	fn func(context.Context, map[string]types.AttributeValue) (bool, error),
) error {
	snapshot := in.ExclusiveStartKey
	defer func() { in.ExclusiveStartKey = snapshot }()

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

// QueryOne executes a query and calls fn for the first item in the result set.
func QueryOne(
	ctx context.Context,
	client *dynamodb.Client,
	m func(any) []func(*dynamodb.Options),
	in *dynamodb.QueryInput,
	fn func(context.Context, map[string]types.AttributeValue) error,
) (bool, error) {
	if in.Limit == nil || *in.Limit != 1 {
		panic("QueryOne() requires a query input with a limit of 1")
	}

	ok := false
	err := query(
		ctx,
		client,
		m,
		in,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			ok = true
			return false, fn(ctx, item)
		},
	)

	return ok, err
}
