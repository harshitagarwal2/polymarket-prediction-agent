CREATE TABLE polymarket_markets (
  market_id TEXT PRIMARY KEY,
  condition_id TEXT,
  token_id_yes TEXT,
  token_id_no TEXT,
  title TEXT NOT NULL,
  description TEXT,
  event_slug TEXT,
  market_slug TEXT,
  category TEXT,
  end_time TIMESTAMPTZ,
  status TEXT NOT NULL,
  raw_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE polymarket_bbo (
  market_id TEXT PRIMARY KEY REFERENCES polymarket_markets(market_id),
  best_bid_yes NUMERIC(10,6),
  best_bid_yes_size NUMERIC(18,6),
  best_ask_yes NUMERIC(10,6),
  best_ask_yes_size NUMERIC(18,6),
  midpoint_yes NUMERIC(10,6),
  spread_yes NUMERIC(10,6),
  book_ts TIMESTAMPTZ NOT NULL,
  source_age_ms BIGINT NOT NULL,
  raw_hash TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE sportsbook_events (
  sportsbook_event_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  sport TEXT NOT NULL,
  league TEXT,
  home_team TEXT,
  away_team TEXT,
  start_time TIMESTAMPTZ NOT NULL,
  raw_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE sportsbook_odds (
  sportsbook_event_id TEXT REFERENCES sportsbook_events(sportsbook_event_id),
  source TEXT NOT NULL,
  market_type TEXT NOT NULL,
  selection TEXT NOT NULL,
  price_decimal NUMERIC(12,6),
  implied_prob NUMERIC(12,6),
  overround NUMERIC(12,6),
  quote_ts TIMESTAMPTZ NOT NULL,
  source_age_ms BIGINT NOT NULL,
  raw_json JSONB NOT NULL,
  PRIMARY KEY (sportsbook_event_id, source, market_type, selection, quote_ts)
);

CREATE TABLE market_mappings (
  mapping_id BIGSERIAL PRIMARY KEY,
  polymarket_market_id TEXT NOT NULL REFERENCES polymarket_markets(market_id),
  sportsbook_event_id TEXT NOT NULL REFERENCES sportsbook_events(sportsbook_event_id),
  sportsbook_market_type TEXT NOT NULL,
  normalized_market_type TEXT NOT NULL,
  match_confidence NUMERIC(5,4) NOT NULL,
  resolution_risk NUMERIC(5,4) NOT NULL,
  mismatch_reason TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE source_health (
  source_name TEXT PRIMARY KEY,
  last_seen_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  stale_after_ms BIGINT NOT NULL,
  status TEXT NOT NULL,
  details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE fair_values (
  market_id TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  fair_yes_prob NUMERIC(10,6) NOT NULL,
  lower_prob NUMERIC(10,6) NOT NULL,
  upper_prob NUMERIC(10,6) NOT NULL,
  book_dispersion NUMERIC(10,6) NOT NULL,
  data_age_ms BIGINT NOT NULL,
  source_count INT NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  PRIMARY KEY (market_id, as_of, model_name, model_version)
);

CREATE TABLE opportunities (
  market_id TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  side TEXT NOT NULL,
  edge_after_costs_bps NUMERIC(10,4) NOT NULL,
  fillable_size NUMERIC(18,6) NOT NULL,
  confidence NUMERIC(10,6) NOT NULL,
  blocked_reason TEXT,
  fair_value_ref TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (market_id, as_of, side)
);

CREATE TABLE trade_attribution (
  trade_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  expected_edge_bps NUMERIC(10,4),
  realized_edge_bps NUMERIC(10,4),
  slippage_bps NUMERIC(10,4),
  pnl NUMERIC(18,6),
  model_error NUMERIC(10,6),
  stale_data_flag BOOLEAN NOT NULL,
  mapping_risk NUMERIC(10,6),
  notes JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE model_registry (
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  feature_spec JSONB NOT NULL,
  metrics JSONB NOT NULL,
  artifact_uri TEXT NOT NULL,
  PRIMARY KEY (model_name, model_version)
);
