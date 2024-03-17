# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

This release changes the `journal.Store` and `journal.Journal` interfaces to be
generic over records of type `T`, and the `kv.Store` and `kv.Keyspace` to be
generic over types `K` and `V`, thereby making the `typedjournal` and `typedkv`
packages obsolete.

This eliminates the need for duplicating all of the generic algorithms (such as
`Search()`, `LastRecord()`, etc) for both binary and typed implementations.

### Added

- Added `journal.BinaryStore`, `BinaryJournal` and `BinaryRangeFunc` aliases,
  equivalent to prior (non-generic) definitions of `Store`, `Journal` and
  `RangeFunc`, respectively.
- Added `kv.BinaryStore`, `BinaryKeyspace` and `BinaryRangeFunc` aliases,
  equivalent to prior (non-generic) definitions of `Store`, `Keyspace` and
  `RangeFunc`, respectively.
- Added `marshal.ProtocolBuffers` marshaler implementation.
- Added `journal.NewMarshalingStore()` and `kv.NewMarshalingStore()`, which wrap
  binary implementations with a `marshal.Marshaler`, serving as a replacement
  for the `typedjournal` and `typedkv` packages, respectively.

### Changed

- **[BC]** Changed `journal` and `kv` interfaces to be generic.
- **[BC]** Moved `typedmarshaler` package to `marshal` at the root of the module.

### Removed

- **[BC]** Removed `typedjournal`, `typedkv` and `typedmarshaler` packages.

## [0.7.0] - 2024-03-17

### Changed

- **[BC]** The `pgjournal` implementation now supports the full range of
  unsigned `journal.Position` values. The `uint64` value-space is "mapped" onto
  the `int64` space such that `uint64(0) == math.MinInt64`, thereby preserving
  order. The `journal_record.position` column has been renamed to
  `encoded_position` to make it clearer that the value can not be used as-is.

### Fixed

- Calling `Range()` on an empty journal now correctly returns
  `journal.ErrNotFound`. This issue affected all journal implementations.

### Removed

- **[BC]** Removed `journal.MaxPosition`. All implementations now support the
  full unsigned 64-bit range of `journal.Position` values. Technically, the
  `memoryjournal` implementation is limited to `math.MaxInt` non-truncated
  records at any given time, but this is not practical anyway.

## [0.6.0] - 2024-03-16

### Added

- Added `journal.RunBenchmarks()` and `kv.RunBenchmarks()` to run generic
  benchmarks for a journal or key-value store implementation.

### Changed

- **[BC]** The PostgreSQL drivers `pgjournal` and `pgkv` now assign each journal
  and keyspace a sequential ID to avoid repeating the journal/keyspace name in
  every row.

### Removed

- **[BC]** Removed `pgjournal.CreateSchema()` and `pgkv.CreateSchema()`. The
  schema creation is now managed at runtime by the journal and keyspace stores.
  This change is in preparation for the stores also managing other schema
  changes, such as table partitioning.

### Fixed

- PostgreSQL schema creation is now performed within a transaction.

## [0.5.0] - 2024-03-12

### Added

- Added `typedjournal.IsEmpty()`, which returns `true` if a journal currently has no records.
- Added `typedjournal.IsFresh()`, which returns `true` if a journal has never been written to.
- Added `typedjournal.FirstRecord()` and `LastRecord()`.
- Added `typedmarshaler.Zero()`

### Changed

- **[BC]** Changed `typedjournal.Journal` to use pointer receivers
- **[BC]** Changed `typedkv.Keyspace` to use pointer receivers

## [0.4.0] - 2024-03-11

### Changed

- **[BC]** Moved `typedjournal` and `typedkv` packages into `adaptor` directory.

### Added

- Added `typedmarshaler` package.
- Added `journal.IsEmpty()`, which returns `true` if a journal currently has no records.
- Added `journal.IsFresh()`, which returns `true` if a journal has never been written to.
- Added `journal.FirstRecord()` and `LastRecord()`.

### Removed

- **[BC]** Removed `typedjournal.Marshaler` and `typedkv.Marshaler`.
  Use `typedmarshaler.Marshaler` instead.

## [0.3.0] - 2024-03-09

### Added

- Added `journal.BinarySearch()`
- Added `typedjournal` package, a generic wrapper around a `journal.Store`
- Added `typedkv` package, a generic wrapper around a `kv.Store`

## [0.2.1] - 2024-03-02

### Added

- Added `journal.WithTelemetry()` and `kv.WithTelemetry()`, which add logging,
  tracing and metrics to an existing journal or key-value store, respectively.

## [0.2.0] - 2024-03-02

### Changed

- **[BC]** Moved `postgres.JournalStore` to `pgjournal.Store`
- **[BC]** Moved `postgres.KeyValueStore` to `pgkv.Store`
- **[BC]** Moved `memory.JournalStore` to `memoryjournal.Store`
- **[BC]** Moved `memory.KeyValueStore` to `memorykv.Store`
- **[BC]** Moved `dynamodb.JournalStore` to `dynamojournal.Store`
- **[BC]** Moved `dynamodb.KeyValueStore` to `dynamokv.Store`

## [0.1.0] - 2023-10-16

### Added

- Added `journal` abstraction
- Added `kv` abstraction
- Added DynamoDB implementations
- Added PostgreSQL implementations
- Added in-memory test implementations

<!-- references -->

[Unreleased]: https://github.com/dogmatiq/persistencekit
[0.1.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.1.0
[0.2.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.2.0
[0.2.1]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.2.1
[0.3.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.3.0
[0.4.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.4.0
[0.5.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.5.0
[0.6.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.6.0
[0.7.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.7.0

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
