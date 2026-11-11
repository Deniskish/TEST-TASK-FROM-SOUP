CREATE TABLE IF NOT EXISTS jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    token bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error text NOT NULL DEFAULT '',
    CHECK (attempts <= max_attempts),
    CHECK ((status = 'running') = (lease_until IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_queued ON jobs(id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_expired ON jobs(lease_until, id) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS job_effects (
    job_id bigint PRIMARY KEY REFERENCES jobs(id),
    token bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result jsonb NOT NULL
);
