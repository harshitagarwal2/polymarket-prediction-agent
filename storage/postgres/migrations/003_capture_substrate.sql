CREATE TABLE IF NOT EXISTS raw_capture_events (
  capture_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  layer TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_key TEXT,
  operation TEXT NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_health_events (
  event_id BIGSERIAL PRIMARY KEY,
  source_name TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  success BOOLEAN NOT NULL DEFAULT TRUE,
  stale_after_ms BIGINT NOT NULL,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS capture_checkpoints (
  checkpoint_name TEXT NOT NULL,
  source_name TEXT NOT NULL,
  checkpoint_value TEXT,
  checkpoint_ts TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (checkpoint_name, source_name)
);

CREATE INDEX IF NOT EXISTS idx_raw_capture_events_source_time
  ON raw_capture_events (source, layer, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_capture_events_entity
  ON raw_capture_events (entity_type, entity_key);

CREATE INDEX IF NOT EXISTS idx_source_health_events_source_time
  ON source_health_events (source_name, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_capture_checkpoints_updated_at
  ON capture_checkpoints (updated_at DESC);
