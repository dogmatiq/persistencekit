# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[keep a changelog]: https://keepachangelog.com/en/1.0.0/
[semantic versioning]: https://semver.org/spec/v2.0.0.html
[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs

## [Unreleased]

### Added

- Added `set.WithInterceptor()` to allow intercepting operations on sets.
- Added `kv.WithInterceptor()` to allow intercepting operations on keyspaces.
- Added `journal.WithInterceptor()` to allow intercepting operations on journals.

### Removed

- **[BC]** Removed hooks from `memoryset.Store` and `BinaryStore`. Use
  `set.WithInterceptor()` instead.
- **[BC]** Removed hooks from `memorykv.Store` and `BinaryStore`. Use
  `kv.WithInterceptor()` instead.
- **[BC]** Removed hooks from `memoryjournal.Store`. Use
  `journal.WithInterceptor()` instead.

## [0.12.0] - 2025-12-04

### Added

- Added `set.Set.Range()` method to iterate over all members of a set.

### Changed

- **[BC]** Renamed `pgset` table from `set_value` to `set_member` to match terminology elsewhere.
- **[BC]** Renamed `dynamoset` attribute from `V` (value) to `M` (member) to match terminology elsewhere.

## [0.11.2] - 2025-12-02

### Changed

- Change telemetry to use `enginekit/telemetry` package.

## [0.11.1] - 2025-11-23

### Fixed

- DynamoDB based implementations now wait for table creation to complete before
  returning from `dynamojournal.NewBinaryStore()` and
  `dynamokv.NewBinaryStore()`. This prevents potential issues due to the
  asynchronous nature of DynamoDB table creation, which is not evident when
  using the local DynamoDB emulator.

## [0.11.0] - 2025-05-31

### Changed

- **[BC]** The `journal.WithTelemetry()`, `kv.WithTelemetry()` and
  `set.WithTelemetry()` functions now require an OpenTelemetry `LoggerProvider`
  instead of an `slog.Logger`. This change means that all persistencekit
  telemetry is now "OpenTelemetry native", ensuring that all log messages are
  correlated with spans when tracing is enabled.
- Changed metric instruments to use more specific names, such as `value_size`
  instead of more generic/network-level names such as `io`.
- Added `misses` metric, for `kv.Keyspace.Get()` operations that do not find a
  value in the keyspace.

## [0.10.2] - 2025-05-29

### Added

- Added `set.NewMarshalingStore()`, which wraps a `set.BinaryStore` with a
  `marshaler.Marshaler` to store values of arbitrary types.

## [0.10.1] - 2025-05-29

### Added

- Added `set` package, which provides an abstraction of named sets of values of
  arbitrary type.
- Added `memoryset` package, a memory-based implementation of `set.Store`.
- Added `dynamoset` package, a DynamoDB-based implementation of `set.BinaryStore`.
- Added `pgset` package, a PostgreSQL-based implementation of `set.BinaryStore`.

### Fixed

- Fixed issue telemetry/tracing issue with `kv.Keyspace.Set()` that would cause
  a (potentially unreadable) _value_ to be added to the span attributes when the
  _key_ was human-readable.

## [0.10.0] - 2024-09-24

### Added

- Added `journal.Interval` type, to represent a half-open range of positions.
- Added `journal.RecordNotFoundError`, `ValueNotFoundError` and `ConflictError`.

### Changed

- **[BC]** `journal.Journal.Bounds()` now returns an `Interval` instead of a two
  `Position` values.
- **[BC]** `journal.Search()`, `RangeFromSearchResult()`, `Scan()` and
  `ScanFromSearchResult()` now accept an `Interval` parameter instead of two
  `Position` values.

### Removed

- **[BC]** Removed `journal.ErrConflict` and `ErrNotFound`. Use `IsConflict()`
  and `IsNotFound()` to test for specific errors instead.

## [0.9.3] - 2024-04-03

### Added

- Added `journal.AppendWithConflictResolution()`.
- Added `journal.IgnoreNotFound()`.

## [0.9.2] - 2024-03-27

### Added

- Added `journal.RangeFromSearchResult()`

## [0.9.1] - 2024-03-19

### Added

- Added property-based tests for journal and key/value store implementations
  using [`rapid`](https://github.com/flyingmutant/rapid). These new tests
  uncovered the bugs described below.

### Fixed

- Fixed issue with `memoryjournal` `Range()` implementation that would pass the
  wrong position to the `RangeFunc` after the journal was truncated.
- Fixed issue with `dynamojournal` `Bounds()` implementation that could
  potentially report the wrong lower bound after truncating (as of 0.9.0).

### Changed

- `dynamojournal` now cleans up (hard-deletes) records that have been marked as
  truncated when scanning for lower bounds.

## [0.9.0] - 2024-03-18

### Added

- Added `dynamojournal.NewBinaryStore()` and `dynamokv.NewBinaryStore()`.
- Added `Option` type and `WithRequestHook()` option to `dynamojournal` and
  `dynamokv`. The request hook is a more flexible replacement for the
  request-type-specific decorator fields that were on the `BinaryStore` structs,
  which were removed in this release.

### Removed

- **[BC]** Removed `dynamojournal.BinaryStore` and `dynamokv.BinaryStore` use
  each package's `NewBinaryStore()` function instead.
- **[BC]** Removed `dynamojournal.CreateTable()` and `dynamokv.CreateTable()`.
  The table creation is now managed at runtime by the journal and keyspace
  stores.

### Fixed

- The `pgjournal` and `dynamojournal` implementations can now correctly truncate
  all journal records. Previously they would only allow truncation up to but not
  including the most recent record.

## [0.8.0] - 2024-03-17

This release changes the `journal.Store` and `journal.Journal` interfaces to be
generic types, parameterized over records of type `T`, and the `kv.Store` and
`kv.Keyspace` to be parameterized over types `K` and `V`, thereby making the
`typedjournal` and `typedkv` packages obsolete.

This eliminates the need for duplicating all of the generic algorithms (such as
`Search()`, `LastRecord()`, etc) for both binary and typed implementations.

### Added

- Added `journal.BinaryStore`, `BinaryJournal` and `BinaryRangeFunc` aliases,
  equivalent to prior (non-generic) definitions of `Store`, `Journal` and
  `RangeFunc`, respectively.
- Added `kv.BinaryStore`, `BinaryKeyspace` and `BinaryRangeFunc` aliases,
  equivalent to prior (non-generic) definitions of `Store`, `Keyspace` and
  `RangeFunc`, respectively.
- Added `marshaler.ProtocolBuffers` marshaler.
- Added `marshaler.Bool` marshaler.
- Added `journal.NewMarshalingStore()` and `kv.NewMarshalingStore()`, which wrap
  binary implementations with a `marshaler.Marshaler`, serving as a replacement
  for the `typedjournal` and `typedkv` packages, respectively.
- Added `journal.Scan()` and `ScanFromSearchResult()`

### Changed

- **[BC]** Changed `journal` and `kv` interfaces to be generic.
- **[BC]** Moved `typedmarshaler` package to `marshaler` at the root of the module.

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
[0.8.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.8.0
[0.9.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.9.0
[0.9.1]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.9.1
[0.9.2]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.9.2
[0.9.3]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.9.3
[0.10.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.10.0
[0.10.1]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.10.1
[0.10.2]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.10.2
[0.11.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.11.0
[0.11.1]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.11.1
[0.11.2]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.11.2
[0.12.0]: https://github.com/dogmatiq/persistencekit/releases/tag/v0.12.0

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
