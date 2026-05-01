package xs3

import (
	"context"
	"errors"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xaws"
)

const (
	// TombstoneLifecycleRuleID is the ID of the S3 lifecycle rule that
	// automatically expires tombstone objects.
	TombstoneLifecycleRuleID = "dogmatiq.io/persistencekit/tombstone"

	// TombstoneTagKey and TombstoneTagValue are the S3 object tag applied to
	// tombstone objects. S3 lifecycle rules can only filter by tag (not
	// content length), so tags are required to target the auto-expiry rule.
	TombstoneTagKey   = "dogmatiq.io/persistencekit/tombstone"
	TombstoneTagValue = "true"
)

// TombstoneTagging is the URL-encoded tag string applied to tombstone objects.
var TombstoneTagging = aws.String(url.Values{TombstoneTagKey: []string{TombstoneTagValue}}.Encode())

// EnsureTombstoneLifecycleRule adds an S3 lifecycle rule to expire tombstone
// objects if one is not already present. It reads the existing rules before
// writing to avoid clobbering any user-managed rules.
func EnsureTombstoneLifecycleRule(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	onRequest func(any) []func(*s3.Options),
) error {
	res, err := xaws.Do(
		ctx,
		client.GetBucketLifecycleConfiguration,
		onRequest,
		&s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
		},
	)

	var rules []types.LifecycleRule
	if err != nil {
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "NoSuchLifecycleConfiguration" {
			return err
		}
	} else {
		for _, r := range res.Rules {
			if aws.ToString(r.ID) == TombstoneLifecycleRuleID {
				return nil // Rule already present.
			}
		}
		rules = res.Rules
	}

	rules = append(rules, types.LifecycleRule{
		ID:     aws.String(TombstoneLifecycleRuleID),
		Status: types.ExpirationStatusEnabled,
		Filter: &types.LifecycleRuleFilter{
			Tag: &types.Tag{
				Key:   aws.String(TombstoneTagKey),
				Value: aws.String(TombstoneTagValue),
			},
		},
		Expiration: &types.LifecycleExpiration{
			Days: aws.Int32(1),
		},
	})

	_, err = xaws.Do(
		ctx,
		client.PutBucketLifecycleConfiguration,
		onRequest,
		&s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: rules,
			},
		},
	)

	return err
}
