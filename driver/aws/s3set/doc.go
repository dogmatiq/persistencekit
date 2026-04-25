// Package s3set provides a [set.BinaryStore] implementation that persists to an
// S3 bucket.
//
// # IAM Permissions
//
// The following IAM actions are required on the S3 bucket:
//   - s3:GetObject
//   - s3:PutObject
//   - s3:ListBucket
//
// Removed members are marked with placeholder objects that are removed
// automatically by an S3 lifecycle rule. The store ensures this rule is
// present, which requires the following additional actions:
//   - s3:GetLifecycleConfiguration
//   - s3:PutLifecycleConfiguration
//
// If the bucket does not already exist, the store attempts to create it
// automatically, which requires the following additional action:
//   - s3:CreateBucket
//
// The store's Provision method can be called to trigger provisioning ahead of
// time.
package s3set
