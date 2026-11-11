ALTER TABLE jobs ADD COLUMN IF NOT EXISTS trace_context jsonb NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS outbox (
 job_id bigint PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
 trace_context jsonb NOT NULL DEFAULT '{}',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(created_at) WHERE published_at IS NULL;
CREATE OR REPLACE FUNCTION enqueue_job() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.status='queued' THEN
 INSERT INTO outbox(job_id,trace_context) VALUES(NEW.id,NEW.trace_context)
 ON CONFLICT(job_id) DO UPDATE SET published_at=NULL,created_at=clock_timestamp();
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS enqueue_job ON jobs;
CREATE TRIGGER enqueue_job AFTER INSERT OR UPDATE OF status ON jobs FOR EACH ROW EXECUTE FUNCTION enqueue_job();
INSERT INTO outbox(job_id,trace_context) SELECT id,trace_context FROM jobs WHERE status IN ('queued','running') ON CONFLICT DO NOTHING;
