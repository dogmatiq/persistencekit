# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## Unreleased

### Added

- Added `typedjournal.IsEmpty()`, which returns `true` if a journal currently has no records.
- Added `typedjournal.IsFresh()`, which returns `true` if a journal has never been written to.
- Added `typedjournal.FirstRecord()` and `LastRecord()`.

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

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
