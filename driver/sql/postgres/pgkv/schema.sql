CREATE SCHEMA IF NOT EXISTS persistencekit;

CREATE TABLE
    IF NOT EXISTS persistencekit.kv (
        keyspace TEXT NOT NULL,
        key BYTEA NOT NULL,
        value BYTEA NOT NULL,
        PRIMARY KEY (keyspace, key)
    )
