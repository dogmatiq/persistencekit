package awsx

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Do executes an AWS API request.
//
// fn is a function that is called to execute the request, typically a method on
// a *dynamodb.DynamoDB client.
//
// m is a function that mutates the input value before it is sent and returns
// any options that should be used when sending the request.
func Do[In, Out any](
	ctx context.Context,
	fn func(context.Context, *In, ...func(*dynamodb.Options)) (Out, error),
	m func(any) []func(*dynamodb.Options),
	in *In,
) (out Out, err error) {
	var options []func(*dynamodb.Options)
	if m != nil {
		options = m(in)
	}
	return fn(ctx, in, options...)
}
