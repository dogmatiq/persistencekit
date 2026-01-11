CREATE SCHEMA IF NOT EXISTS persistencekit;

CREATE TABLE
    IF NOT EXISTS persistencekit.keyspace (
        id BIGSERIAL NOT NULL,
        name TEXT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE (name)
    );

CREATE TABLE
    IF NOT EXISTS persistencekit.keyspace_pair (
        keyspace_id BIGINT NOT NULL,
        key BYTEA NOT NULL,
        value BYTEA NOT NULL,
        revision BIGINT NOT NULL DEFAULT 1,
        PRIMARY KEY (keyspace_id, key),

        CHECK (octet_length(value) > 0),
        CHECK (revision > 0)
    );
