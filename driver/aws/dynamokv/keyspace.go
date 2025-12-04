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

	attr struct {
		Keyspace types.AttributeValueMemberS
		Key      types.AttributeValueMemberB
		Value    types.AttributeValueMemberB
	}

	request struct {
		Get    dynamodb.GetItemInput
		Has    dynamodb.GetItemInput
		Range  dynamodb.QueryInput
		Set    dynamodb.PutItemInput
		Delete dynamodb.DeleteItemInput
	}
}

func (ks *keyspace) Name() string {
	return ks.attr.Keyspace.Value
}

func (ks *keyspace) Get(ctx context.Context, k []byte) ([]byte, error) {
	ks.attr.Key.Value = k

	out, err := awsx.Do(
		ctx,
		ks.Client.GetItem,
		ks.OnRequest,
		&ks.request.Get,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get keyspace pair: %w", err)
	}
	if out.Item == nil {
		return nil, err
	}

	v, err := dynamox.AsBytes(out.Item, valueAttr)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (ks *keyspace) Has(ctx context.Context, k []byte) (bool, error) {
	ks.attr.Key.Value = k

	out, err := awsx.Do(
		ctx,
		ks.Client.GetItem,
		ks.OnRequest,
		&ks.request.Has,
	)
	if err != nil {
		return false, fmt.Errorf("unable to get keyspace pair: %w", err)
	}

	return out.Item != nil, nil
}

func (ks *keyspace) Set(ctx context.Context, k, v []byte) error {
	if len(v) == 0 {
		return ks.delete(ctx, k)
	}
	return ks.set(ctx, k, v)
}

func (ks *keyspace) set(ctx context.Context, k, v []byte) error {
	ks.attr.Key.Value = k
	ks.attr.Value.Value = v

	if _, err := awsx.Do(
		ctx,
		ks.Client.PutItem,
		ks.OnRequest,
		&ks.request.Set,
	); err != nil {
		return fmt.Errorf("unable to put keyspace pair: %w", err)
	}

	return nil
}

func (ks *keyspace) delete(ctx context.Context, k []byte) error {
	ks.attr.Key.Value = k

	if _, err := awsx.Do(
		ctx,
		ks.Client.DeleteItem,
		ks.OnRequest,
		&ks.request.Delete,
	); err != nil {
		return fmt.Errorf("unable to delete keyspace pair: %w", err)
	}

	return nil
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	if err := dynamox.QueryRange(
		ctx,
		ks.Client,
		ks.OnRequest,
		&ks.request.Range,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			key, err := dynamox.AsBytes(item, keyAttr)
			if err != nil {
				return false, err
			}

			value, err := dynamox.AsBytes(item, valueAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, key, value)
		},
	); err != nil {
		return fmt.Errorf("unable to range over keyspace: %w", err)
	}

	return nil
}

func (ks *keyspace) Close() error {
	return nil
}
