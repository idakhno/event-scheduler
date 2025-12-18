DROP TRIGGER IF EXISTS trg_events_updated_at ON events;
DROP FUNCTION IF EXISTS update_updated_at();
DROP INDEX IF EXISTS idx_events_scheduled_next_run;
DROP TABLE IF EXISTS events;

