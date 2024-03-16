CREATE SCHEMA IF NOT EXISTS persistencekit;

CREATE TABLE
    IF NOT EXISTS persistencekit.journal (
        id BIGSERIAL NOT NULL,
        name TEXT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE (name)
    );

CREATE TABLE
    IF NOT EXISTS persistencekit.journal_record (
        journal_id BIGINT NOT NULL,
        encoded_position BIGINT NOT NULL, -- see `bigint` package
        record BYTEA NOT NULL,
        PRIMARY KEY (journal_id, encoded_position)
    );
