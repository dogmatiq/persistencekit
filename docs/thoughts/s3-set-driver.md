# S3 Set Driver

Implementation notes for a potential `s3set` driver, based on lessons learned
from the `s3kv` driver.

## Core mapping

A set member is a piece of binary data. The natural S3 representation is one
object per member, with the member value encoded as the object key (hex, same
as `s3kv`). The object body can be empty -- there is no value to store, so
a zero-byte `ContentLength: 0` body is ideal.

Object key structure (mirrors `s3kv`):

```
set/<url-path-escaped set name>/<hex member>
```

## Operations

### Has

`HeadObject` on the member's key. Returns true if the object exists and is not
a tombstone.

### Add / TryAdd

`PutObject` with `IfNoneMatch: "*"` to insert only when absent.

- `Add` can skip the condition -- unconditional `PutObject` is idempotent for
  members since the body is always empty.
- `TryAdd` uses `IfNoneMatch: "*"`. A conflict response means the member is
  already present (return false, nil).

The tombstone complication from `s3kv` applies here too. If `IfNoneMatch: "*"`
conflicts, a `HeadObject` is needed to check whether the conflicting object is
a tombstone. If it is, replace it with a real (non-tombstone) object using
`IfMatch: <etag>` as in `s3kv`. The same retry loop applies.

### Remove / TryRemove

Write a tombstone object (empty body, `tombstone=true` metadata, tombstone tag).

- `Remove` is unconditional -- if the object doesn't exist, `IfNoneMatch: "*"`
  writes the tombstone; if it conflicts, check if it's already a tombstone
  (done, return nil) or a real member (replace with tombstone via `IfMatch`).
- `TryRemove` needs to know whether a member was actually removed. Check
  `HeadObject` before writing: if absent or tombstone, return false; if
  present, write tombstone with `IfMatch: <etag>`.

### Range

`ListObjectsV2` with the set prefix, then `HeadObject` each key (no body
needed -- body is always empty so `GetObject` is wasteful). Skip tombstones.
Decode the hex suffix of each object key to recover the member bytes.

Using `HeadObject` instead of `GetObject` during `Range` is a meaningful
improvement over `s3kv`, since set members have no body to read. This also
avoids the `io.ReadAll` vs. tombstone-check ordering issue entirely.

## Tombstone lifecycle

Identical to `s3kv`: a lifecycle rule tagged
`dogmatiq.io/persistencekit/tombstone=true` expires tombstones after 1 day.
The `ensureTombstoneLifecycleRule` helper from `s3kv` can be shared or
duplicated into the set driver. The same `tombstoneMetaKey/Value` and
`tombstoneTagKey/Value` constants apply.

The lifecycle rule and tombstone constants could be pulled up into a shared
`internal/s3tomb` package if both drivers exist in the same module.

## Object key prefix

The store should use a configurable path prefix (default `"set/"`) so a single
bucket can hold both `s3kv` and `s3set` data without collision.

## Concurrency

S3's conditional write primitives (`IfMatch`, `IfNoneMatch`) provide the same
optimistic concurrency as `s3kv`. No additional locking is needed.

The same race between `IfNoneMatch: "*"` and a live tombstone applies and
requires the same check-then-replace retry loop.

## Store / Keyspace structure

Mirrors `s3kv` almost directly:

- `store` holds `Client`, `Bucket`, `OnRequest`, and a `xsync.SucceedOnce`
  for the one-time bucket/lifecycle setup.
- `setimpl` holds `client`, `onRequest`, `name`, `bucket`, and
  `objectKeyPrefix`.
- `NewBinaryStore` and `WithRequestHook` follow the same shape.

## What is simpler vs. s3kv

- No revision tracking -- ETags only needed transiently for conditional writes,
  not returned to callers.
- No value body to read -- `HeadObject` suffices for `Range` and existence
  checks, saving a `GetObject` + `io.ReadAll` per member.
- `SetUnconditional` has no equivalent -- not part of the `Set` interface.
- The conditional write path is simpler: no need to return a new revision after
  `Add`/`Remove`.

## Open questions

- Should `s3set` share the tombstone lifecycle constants with `s3kv` via a
  shared internal package, or keep them duplicated for independence?
- Should tombstone expiry for sets use a different lifecycle rule ID/tag to
  allow different retention periods per resource type?
- Is `HeadObject` per-member in `Range` acceptable at scale? An alternative is
  to store a sentinel body and use `GetObject`, but the tombstone
  metadata-vs-tag duality is already designed for `HeadObject`.
