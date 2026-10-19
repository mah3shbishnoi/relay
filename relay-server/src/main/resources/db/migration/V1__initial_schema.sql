-- V1: core job tables. Mirrors the project doc, section 24.
-- Every state change must also write a job_events row in the same transaction.

CREATE TYPE job_state AS ENUM ('SCHEDULED', 'RUNNING', 'SUCCEEDED', 'DEAD');

CREATE TABLE jobs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  queue            TEXT NOT NULL,
  payload          JSONB NOT NULL,
  payload_hash     TEXT NOT NULL,
  idempotency_key  TEXT,
  state            job_state NOT NULL DEFAULT 'SCHEDULED',
  priority         INT NOT NULL DEFAULT 0,
  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts         INT NOT NULL DEFAULT 0,
  max_attempts     INT NOT NULL DEFAULT 5,
  lease_owner      TEXT,
  lease_expires_at TIMESTAMPTZ,
  fencing_token    BIGINT NOT NULL DEFAULT 0,
  last_error       TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  CONSTRAINT running_has_lease CHECK (
    state <> 'RUNNING' OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
  CONSTRAINT attempts_within_max CHECK (attempts >= 0 AND attempts <= max_attempts)
);

-- Idempotent submit: one row per (queue, idempotency_key).
CREATE UNIQUE INDEX uq_jobs_idempotency
  ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Fast claiming of ready jobs: highest priority first, then earliest run_at.
CREATE INDEX ix_jobs_ready
  ON jobs (queue, priority DESC, run_at) WHERE state = 'SCHEDULED';

-- Reaper scan for expired leases.
CREATE INDEX ix_jobs_leases
  ON jobs (lease_expires_at) WHERE state = 'RUNNING';

CREATE TABLE job_events (
  id            BIGSERIAL PRIMARY KEY,
  job_id        UUID NOT NULL,
  event_type    TEXT NOT NULL,
  attempt       INT,
  fencing_token BIGINT,
  worker_id     TEXT,
  at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  details       JSONB
);

CREATE INDEX ix_job_events_job ON job_events (job_id, id);
