package s3kv

import (
	"context"
	"errors"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
)

const (
	// tombstoneLifecycleRuleID is the ID of the S3 lifecycle rule that
	// automatically expires tombstone objects.
	tombstoneLifecycleRuleID = "dogmatiq.io/persistencekit/tombstone"

	// tombstoneTagKey and tombstoneTagValue are the S3 object tag applied to
	// tombstone objects. S3 lifecycle rules can only filter by tag (not
	// metadata), so tags are required to target the auto-expiry rule.
	tombstoneTagKey   = "dogmatiq.io/persistencekit/tombstone"
	tombstoneTagValue = "true"

	// tombstoneMetaKey and tombstoneMetaValue are the S3 object metadata key
	// and value used to detect tombstones inline. GetObject and HeadObject
	// return metadata in the response body, but not tags -- tags require a
	// separate GetObjectTagging call. The metadata avoids that extra round-trip
	// on every read.
	tombstoneMetaKey   = "tombstone"
	tombstoneMetaValue = "true"
)

var (
	// tombstoneTagging is the URL-encoded tag string applied to tombstone objects.
	tombstoneTagging = aws.String(url.QueryEscape(tombstoneTagKey) + "=" + tombstoneTagValue)

	// tombstoneMetadata is the S3 object metadata applied to tombstone objects.
	tombstoneMetadata = map[string]string{tombstoneMetaKey: tombstoneMetaValue}
)

// isTombstone returns true if the given object metadata indicates a tombstone.
func isTombstone(metadata map[string]string) bool {
	return metadata[tombstoneMetaKey] == tombstoneMetaValue
}

// ensureTombstoneLifecycleRule adds an S3 lifecycle rule to expire tombstone
// objects if one is not already present. It reads the existing rules before
// writing to avoid clobbering any user-managed rules.
func ensureTombstoneLifecycleRule(ctx context.Context, s *store) error {
	res, err := awsx.Do(
		ctx,
		s.Client.GetBucketLifecycleConfiguration,
		s.OnRequest,
		&s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(s.Bucket),
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
			if aws.ToString(r.ID) == tombstoneLifecycleRuleID {
				return nil // Rule already present.
			}
		}
		rules = res.Rules
	}

	rules = append(rules, types.LifecycleRule{
		ID:     aws.String(tombstoneLifecycleRuleID),
		Status: types.ExpirationStatusEnabled,
		Filter: &types.LifecycleRuleFilter{
			Tag: &types.Tag{
				Key:   aws.String(tombstoneTagKey),
				Value: aws.String(tombstoneTagValue),
			},
		},
		Expiration: &types.LifecycleExpiration{
			Days: aws.Int32(1),
		},
	})

	_, err = awsx.Do(
		ctx,
		s.Client.PutBucketLifecycleConfiguration,
		s.OnRequest,
		&s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(s.Bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: rules,
			},
		},
	)

	return err
}
