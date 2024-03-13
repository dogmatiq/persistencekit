CREATE SCHEMA IF NOT EXISTS persistencekit;

CREATE TABLE
    IF NOT EXISTS persistencekit.journal (
        name TEXT NOT NULL,
        position BIGINT NOT NULL,
        record BYTEA NOT NULL,
        PRIMARY KEY (name, position)
    );
