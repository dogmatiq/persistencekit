package dynamokv

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/kv"
)

type keyspace struct {
	Client             *dynamodb.Client
	DecorateGetItem    func(*dynamodb.GetItemInput) []func(*dynamodb.Options)
	DecorateQuery      func(*dynamodb.QueryInput) []func(*dynamodb.Options)
	DecoratePutItem    func(*dynamodb.PutItemInput) []func(*dynamodb.Options)
	DecorateDeleteItem func(*dynamodb.DeleteItemInput) []func(*dynamodb.Options)

	name  *types.AttributeValueMemberS
	key   *types.AttributeValueMemberB
	value *types.AttributeValueMemberB

	getRequest    dynamodb.GetItemInput
	hasRequest    dynamodb.GetItemInput
	queryRequest  dynamodb.QueryInput
	putRequest    dynamodb.PutItemInput
	deleteRequest dynamodb.DeleteItemInput
}

func (ks *keyspace) Get(ctx context.Context, k []byte) ([]byte, error) {
	ks.key.Value = k

	out, err := awsx.Do(
		ctx,
		ks.Client.GetItem,
		ks.DecorateGetItem,
		&ks.getRequest,
	)
	if err != nil || out.Item == nil {
		return nil, err
	}

	v, err := dynamox.AttrAs[*types.AttributeValueMemberB](out.Item, valueAttr)
	if err != nil {
		return nil, err
	}

	return v.Value, nil

}

func (ks *keyspace) Has(ctx context.Context, k []byte) (bool, error) {
	ks.key.Value = k

	out, err := awsx.Do(
		ctx,
		ks.Client.GetItem,
		ks.DecorateGetItem,
		&ks.hasRequest,
	)
	if err != nil {
		return false, err
	}

	return out.Item != nil, nil
}

func (ks *keyspace) Set(ctx context.Context, k, v []byte) error {
	if v == nil {
		return ks.delete(ctx, k)
	}

	return ks.set(ctx, k, v)
}

func (ks *keyspace) set(ctx context.Context, k, v []byte) error {
	ks.key.Value = k
	ks.value.Value = v

	_, err := awsx.Do(
		ctx,
		ks.Client.PutItem,
		ks.DecoratePutItem,
		&ks.putRequest,
	)

	return err
}

func (ks *keyspace) delete(ctx context.Context, k []byte) error {
	ks.key.Value = k

	_, err := awsx.Do(
		ctx,
		ks.Client.DeleteItem,
		ks.DecorateDeleteItem,
		&ks.deleteRequest,
	)

	return err
}

func (ks *keyspace) Range(
	ctx context.Context,
	fn kv.RangeFunc,
) error {
	ks.queryRequest.ExclusiveStartKey = nil

	for {
		out, err := awsx.Do(
			ctx,
			ks.Client.Query,
			ks.DecorateQuery,
			&ks.queryRequest,
		)
		if err != nil {
			return err
		}

		for _, item := range out.Items {
			key, err := dynamox.AttrAs[*types.AttributeValueMemberB](item, keyAttr)
			if err != nil {
				return err
			}

			value, err := dynamox.AttrAs[*types.AttributeValueMemberB](item, valueAttr)
			if err != nil {
				return err
			}

			ok, err := fn(ctx, key.Value, value.Value)
			if !ok || err != nil {
				return err
			}
		}

		if out.LastEvaluatedKey == nil {
			return nil
		}

		ks.queryRequest.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (ks *keyspace) Close() error {
	return nil
}
