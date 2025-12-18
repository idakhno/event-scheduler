CREATE TABLE IF NOT EXISTS events (
  id              UUID PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  type            TEXT NOT NULL,
  payload         JSONB NOT NULL,
  status          TEXT NOT NULL, -- received|scheduled|processing|done|failed|dead
  retry_count     INT NOT NULL DEFAULT 0,
  next_run_at     TIMESTAMPTZ,
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for filtering events that need to be executed, limited by status and time
CREATE INDEX idx_events_scheduled_next_run
  ON events(next_run_at)
  WHERE status IN ('scheduled','processing') AND next_run_at IS NOT NULL;

-- Trigger function to update updated_at
CREATE OR REPLACE FUNCTION update_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_updated_at
BEFORE UPDATE ON events
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

