// Package dynamoset provides a [set.BinaryStore] implementation that persists
// to a DynamoDB table.
//
// # IAM Permissions
//
// The following IAM actions are required on the DynamoDB table:
//   - dynamodb:DescribeTable
//   - dynamodb:GetItem
//   - dynamodb:PutItem
//   - dynamodb:DeleteItem
//   - dynamodb:Query
//
// If the table does not already exist, the store attempts to create it
// automatically, which requires the following additional action:
//   - dynamodb:CreateTable
//
// [BinaryStore.Provision] can be called to trigger provisioning ahead of time.
package dynamoset
