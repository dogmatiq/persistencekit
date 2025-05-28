CREATE SCHEMA IF NOT EXISTS persistencekit;

CREATE TABLE
    IF NOT EXISTS persistencekit.set (
        id BIGSERIAL NOT NULL,
        name TEXT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE (name)
    );

CREATE TABLE
    IF NOT EXISTS persistencekit.set_value (
        set_id BIGINT NOT NULL,
        value BYTEA NOT NULL,
        PRIMARY KEY (set_id, value)
    );
