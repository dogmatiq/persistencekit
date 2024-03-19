package dynamokv

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/kv"
)

type keyspace struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

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
		ks.OnRequest,
		&ks.getRequest,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get keyspace pair: %w", err)
	}
	if out.Item == nil {
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
		ks.OnRequest,
		&ks.hasRequest,
	)
	if err != nil {
		return false, fmt.Errorf("unable to get keyspace pair: %w", err)
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

	if _, err := awsx.Do(
		ctx,
		ks.Client.PutItem,
		ks.OnRequest,
		&ks.putRequest,
	); err != nil {
		return fmt.Errorf("unable to put keyspace pair: %w", err)
	}

	return nil
}

func (ks *keyspace) delete(ctx context.Context, k []byte) error {
	ks.key.Value = k

	if _, err := awsx.Do(
		ctx,
		ks.Client.DeleteItem,
		ks.OnRequest,
		&ks.deleteRequest,
	); err != nil {
		return fmt.Errorf("unable to delete keyspace pair: %w", err)
	}

	return nil
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	if err := dynamox.Range(
		ctx,
		ks.Client,
		ks.OnRequest,
		&ks.queryRequest,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			key, err := dynamox.AttrAs[*types.AttributeValueMemberB](item, keyAttr)
			if err != nil {
				return false, err
			}

			value, err := dynamox.AttrAs[*types.AttributeValueMemberB](item, valueAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, key.Value, value.Value)
		},
	); err != nil {
		return fmt.Errorf("unable to range over keyspace: %w", err)
	}
	return nil
}

func (ks *keyspace) Close() error {
	return nil
}
