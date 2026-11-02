CREATE TABLE IF NOT EXISTS files (
 id text PRIMARY KEY, filename text NOT NULL, storage_key text NOT NULL UNIQUE,
 size_bytes bigint NOT NULL CHECK(size_bytes >= 0), sha256 text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jobs (
 id text PRIMARY KEY, file_id text NOT NULL REFERENCES files(id),
 idempotency_key text NOT NULL UNIQUE, request_hash text NOT NULL,
 state text NOT NULL CHECK(state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
 snapshot jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS queued_jobs ON jobs(created_at,id) WHERE state='QUEUED';
CREATE TABLE IF NOT EXISTS job_attempts (
 id text PRIMARY KEY, job_id text NOT NULL REFERENCES jobs(id), number integer NOT NULL,
 worker_id text NOT NULL, token_hash text NOT NULL, expires_at timestamptz NOT NULL,
 state text NOT NULL CHECK(state IN ('RUNNING','SUCCEEDED','FAILED','EXPIRED','CANCELLED')),
 completion_hash text, UNIQUE(job_id,number)
);
CREATE INDEX IF NOT EXISTS active_attempts ON job_attempts(expires_at) WHERE state='RUNNING';
CREATE TABLE IF NOT EXISTS job_events (
 job_id text NOT NULL REFERENCES jobs(id), sequence bigint NOT NULL, kind text NOT NULL,
 snapshot jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(job_id,sequence)
);
CREATE TABLE IF NOT EXISTS artifacts (
 job_id text NOT NULL REFERENCES jobs(id), attempt_id text NOT NULL REFERENCES job_attempts(id),
 kind text NOT NULL CHECK(kind IN ('summary','errors')), storage_key text NOT NULL UNIQUE,
 size_bytes bigint NOT NULL, sha256 text NOT NULL, PRIMARY KEY(job_id,kind)
);
