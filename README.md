<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://dogmatiq.io/res/img/dynamic/repo/persistencekit/banner/dark.svg">
  <img alt="Dogmatiq persistencekit" src="https://dogmatiq.io/res/img/dynamic/repo/persistencekit/banner/light.svg" height="44">
</picture>

Abstract persistence primitives for use in
[Dogma](https://github.com/dogmatiq/dogma) engines, projections, etc.

[![API reference](https://dogmatiq.io/res/img/dynamic/repo/persistencekit/badge/docs.svg)](https://pkg.go.dev/github.com/dogmatiq/persistencekit)
[![Latest version](https://dogmatiq.io/res/img/dynamic/repo/persistencekit/badge/semver.svg)](https://github.com/dogmatiq/persistencekit/releases)
[![Build status](https://dogmatiq.io/res/img/dynamic/repo/persistencekit/badge/build.svg)](https://github.com/dogmatiq/persistencekit/actions/workflows/ci.yml)
[![Code coverage](https://dogmatiq.io/res/img/dynamic/repo/persistencekit/badge/coverage.svg)](https://codecov.io/github/dogmatiq/persistencekit)

</div>

The persistence toolkit provides a set of relatively low-level persistence
abstractions that can be used to build higher-level storage systems.

The interfaces are designed to be easy to implement by placing a minimal set of
requirements on each implementation.

## Abstractions

- [`Journal`] - an append-only log of binary records with [optimistic concurrency control]
- [`Keyspace`] - a non-transactional binary key/value store
- [`Set`] - a set of unique binary values

## Drivers

Implementations of the above abstractions are called "drivers". Several built-in
drivers are included, each in their own package within the [`driver`] directory.

- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [PostgreSQL](https://www.postgresql.org/) and compatible databases
- In-memory reference/testing implementation

<!-- references-->

[optimistic concurrency control]: https://en.wikipedia.org/wiki/Optimistic_concurrency_control
[`journal`]: journal/journal.go
[`keyspace`]: kv/keyspace.go
[`set`]: set/set.go
[`driver`]: driver
