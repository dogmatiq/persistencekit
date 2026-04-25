// Package s3journal provides an implementation of [journal.BinaryStore] that
// persists to an S3 bucket.
//
// # IAM Permissions
//
// The following IAM actions are required on the S3 bucket:
//   - s3:GetObject
//   - s3:PutObject
//   - s3:DeleteObject
//   - s3:ListBucket
//
// If the bucket does not already exist, the store attempts to create it
// automatically, which requires the following additional action:
//   - s3:CreateBucket
//
// The store's Provision method can be called to trigger provisioning ahead of
// time.
package s3journal
