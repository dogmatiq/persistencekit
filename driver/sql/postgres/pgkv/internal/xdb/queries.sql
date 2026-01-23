-- name: UpsertKeyspace :one
INSERT INTO persistencekit.keyspace (
    name
) VALUES (
    sqlc.arg('name')
) ON CONFLICT (name) DO UPDATE SET
    name = EXCLUDED.name
RETURNING id;

-- name: SelectPair :one
SELECT
    value,
    revision
FROM persistencekit.keyspace_pair
WHERE keyspace_id = sqlc.arg('id')
AND key = sqlc.arg('key');
