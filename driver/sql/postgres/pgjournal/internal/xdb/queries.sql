-- name: UpsertJournal :one
INSERT INTO persistencekit.journal (
    name
) VALUES (
    sqlc.arg('name')
) ON CONFLICT (name) DO UPDATE SET
    name = EXCLUDED.name
RETURNING id;

-- name: UpdateBegin :execrows
UPDATE persistencekit.journal
SET "begin" = sqlc.arg('begin')
WHERE id = sqlc.arg('journal_id')
AND "begin" < sqlc.arg('begin');

-- name: IncrementEnd :execrows
UPDATE persistencekit.journal
SET "end" = "end" + 1
WHERE id = sqlc.arg('journal_id')
AND "end" = sqlc.arg('end');

-- name: SelectBounds :one
SELECT
    "begin",
    "end"
FROM persistencekit.journal
WHERE id = sqlc.arg('journal_id');

-- name: SelectRecord :one
SELECT
    record
FROM persistencekit.journal_record
WHERE journal_id = sqlc.arg('journal_id')
AND position = sqlc.arg('position');

-- name: SelectRecords :many
SELECT
    position,
    record
FROM persistencekit.journal_record
WHERE journal_id = sqlc.arg('journal_id')
AND position >= sqlc.arg('position')
ORDER BY position
LIMIT 500;

-- name: InsertRecord :exec
INSERT INTO persistencekit.journal_record (
    journal_id,
    position,
    record
) VALUES (
    sqlc.arg('journal_id'),
    sqlc.arg('position'),
    sqlc.arg('record')
);

-- name: DeleteRecords :exec
DELETE FROM persistencekit.journal_record
WHERE journal_id = sqlc.arg('journal_id')
AND position < sqlc.arg('end');
