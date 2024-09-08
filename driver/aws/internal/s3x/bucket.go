package s3x

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
)

// CreateBucketIfNotExists creates an S3 bucket if it does not already exist.
func CreateBucketIfNotExists(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	onRequest func(any) []func(*s3.Options),
) error {
	_, err := awsx.Do(
		ctx,
		client.CreateBucket,
		onRequest,
		&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		},
	)
	return IgnoreAlreadyExists(err)
}

// DeleteBucketIfExists deletes an S3 bucket if it exists.
func DeleteBucketIfExists(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	onRequest func(any) []func(*s3.Options),
) (err error) {
	for {
		if _, err := awsx.Do(
			ctx,
			client.DeleteBucket,
			onRequest,
			&s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			},
		); IgnoreNotExists(err) == nil {
			return nil
		}

		for {
			res, err := awsx.Do(
				ctx,
				client.ListObjectsV2,
				onRequest,
				&s3.ListObjectsV2Input{
					Bucket: aws.String(bucket),
				},
			)
			if err != nil {
				return err
			}

			objects := make([]types.ObjectIdentifier, 0, len(res.Contents))
			for _, obj := range res.Contents {
				objects = append(
					objects,
					types.ObjectIdentifier{
						Key: obj.Key,
					},
				)
			}

			if _, err := awsx.Do(
				ctx,
				client.DeleteObjects,
				onRequest,
				&s3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &types.Delete{
						Objects: objects,
						Quiet:   aws.Bool(true),
					},
					BypassGovernanceRetention: aws.Bool(true),
				},
			); err != nil {
				return err
			}

			if !*res.IsTruncated {
				break
			}
		}
	}
}
