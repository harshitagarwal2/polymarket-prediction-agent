CREATE TABLE IF NOT EXISTS polymarket_orders_current (
  order_id TEXT PRIMARY KEY,
  contract_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS polymarket_fills_current (
  fill_key TEXT PRIMARY KEY,
  order_id TEXT NOT NULL,
  contract_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS polymarket_positions_current (
  contract_key TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS polymarket_balance_current (
  balance_key TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_polymarket_orders_current_contract_key
  ON polymarket_orders_current (contract_key);

CREATE INDEX IF NOT EXISTS idx_polymarket_fills_current_contract_key
  ON polymarket_fills_current (contract_key);
