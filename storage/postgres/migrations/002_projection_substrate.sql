ALTER TABLE polymarket_markets
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE polymarket_bbo
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE sportsbook_events
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE sportsbook_odds
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE market_mappings
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE source_health
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE fair_values
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE opportunities
  ADD COLUMN IF NOT EXISTS fair_yes_prob NUMERIC(10,6),
  ADD COLUMN IF NOT EXISTS best_bid_yes NUMERIC(10,6),
  ADD COLUMN IF NOT EXISTS best_ask_yes NUMERIC(10,6),
  ADD COLUMN IF NOT EXISTS edge_buy_bps NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS edge_sell_bps NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS edge_buy_after_costs_bps NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS edge_sell_after_costs_bps NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS blocked_reasons JSONB NOT NULL DEFAULT '{"blocked_reasons": []}'::jsonb,
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE trade_attribution
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE model_registry
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS polymarket_book_snapshots (
  market_id TEXT NOT NULL REFERENCES polymarket_markets(market_id),
  book_ts TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (market_id, book_ts)
);

CREATE TABLE IF NOT EXISTS sportsbook_odds_current (
  sportsbook_event_id TEXT NOT NULL REFERENCES sportsbook_events(sportsbook_event_id),
  source TEXT NOT NULL,
  market_type TEXT NOT NULL,
  selection TEXT NOT NULL,
  quote_ts TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (sportsbook_event_id, source, market_type, selection)
);

CREATE TABLE IF NOT EXISTS market_mappings_current (
  polymarket_market_id TEXT NOT NULL REFERENCES polymarket_markets(market_id),
  sportsbook_event_id TEXT NOT NULL REFERENCES sportsbook_events(sportsbook_event_id),
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (polymarket_market_id, sportsbook_event_id)
);

CREATE TABLE IF NOT EXISTS fair_values_current (
  market_id TEXT PRIMARY KEY REFERENCES polymarket_markets(market_id),
  as_of TIMESTAMPTZ NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS opportunities_current (
  market_id TEXT NOT NULL REFERENCES polymarket_markets(market_id),
  side TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (market_id, side)
);

CREATE INDEX IF NOT EXISTS idx_polymarket_book_snapshots_recorded_at
  ON polymarket_book_snapshots (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_current_quote_ts
  ON sportsbook_odds_current (quote_ts DESC);

CREATE INDEX IF NOT EXISTS idx_market_mappings_current_market
  ON market_mappings_current (polymarket_market_id);

CREATE INDEX IF NOT EXISTS idx_fair_values_current_as_of
  ON fair_values_current (as_of DESC);

CREATE INDEX IF NOT EXISTS idx_opportunities_current_as_of
  ON opportunities_current (as_of DESC);
