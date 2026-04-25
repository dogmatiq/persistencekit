// Package s3kv provides a [kv.BinaryStore] implementation that persists to an
// S3 bucket.
//
// # IAM Permissions
//
// The following IAM actions are required on the S3 bucket:
//   - s3:GetObject
//   - s3:PutObject
//   - s3:ListBucket
//
// If the bucket does not already exist, the store attempts to create it
// automatically, which requires the following additional actions:
//   - s3:CreateBucket
//   - s3:GetLifecycleConfiguration
//   - s3:PutLifecycleConfiguration
//
// [BinaryStore.Provision] can be called to trigger provisioning ahead of time.
package s3kv
