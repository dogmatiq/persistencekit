CREATE SCHEMA IF NOT EXISTS persistencekit;

-- see `commonschema.Uint64` type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.domains
        WHERE domain_schema = 'persistencekit'
        AND domain_name = 'uint64'
    ) THEN

        CREATE DOMAIN persistencekit.uint64
        AS BIGINT
        DEFAULT -1::BIGINT << 63;

    END IF;
END
$$;
