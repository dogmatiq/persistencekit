CREATE TABLE IF NOT EXISTS persistencekit.journal (
    id      BIGSERIAL NOT NULL,
    name    TEXT NOT NULL,
    "begin" persistencekit.uint64 NOT NULL,
    "end"   persistencekit.uint64 NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS persistencekit.journal_record (
    journal_id BIGINT NOT NULL,
    position   persistencekit.uint64 NOT NULL,
    record     BYTEA NOT NULL,

    PRIMARY KEY (journal_id, position)
);
