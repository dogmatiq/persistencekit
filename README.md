<div align="center">

# Dogma Persistence Toolkit

Abstract persistence primitives for use in
[Dogma](https://github.com/dogmatiq/dogma) engines, projections, etc.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/persistencekit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/persistencekit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/persistencekit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/persistencekit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/persistencekit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/persistencekit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/persistencekit)

</div>

The persistence toolkit provides a set of relatively low-level persistence
abstractions that can be used to build higher-level storage systems.

The interfaces are designed to be easy to implement by placing a minimal set of
requirements on each implementation.

## Abstractions

- [`Journal`] - an append-only log of binary records with [optimistic concurrency control]
- [`Keyspace`] - a non-transactional binary key/value store

## Drivers

Implementations of the above primitives are called "driver". Several built-in
drivers are included, each in their own package within the [`driver`] directory.

- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [PostgreSQL](https://www.postgresql.org/) and compatible databases
- In-memory reference/testing implementation

<!-- references-->

[optimistic concurrency control]: https://en.wikipedia.org/wiki/Optimistic_concurrency_control
[`journal`]: journal/journal.go
[`keyspace`]: kv/keyspace.go
[`driver`]: driver
