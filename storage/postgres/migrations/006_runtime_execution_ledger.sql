CREATE TABLE IF NOT EXISTS runtime_cycles (
  cycle_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  selected_market_key TEXT,
  policy_allowed BOOLEAN,
  halted BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trade_decisions (
  decision_id BIGSERIAL PRIMARY KEY,
  cycle_id TEXT NOT NULL REFERENCES runtime_cycles(cycle_id) ON DELETE CASCADE,
  market_id TEXT NOT NULL,
  contract_key TEXT,
  side TEXT,
  fair_value NUMERIC(12, 6),
  market_price NUMERIC(12, 6),
  score NUMERIC(12, 6),
  blocked BOOLEAN NOT NULL DEFAULT FALSE,
  blocked_reason TEXT,
  blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS execution_orders (
  execution_order_id BIGSERIAL PRIMARY KEY,
  cycle_id TEXT NOT NULL REFERENCES runtime_cycles(cycle_id) ON DELETE CASCADE,
  decision_id BIGINT REFERENCES trade_decisions(decision_id) ON DELETE SET NULL,
  order_id TEXT,
  contract_key TEXT,
  accepted BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL,
  message TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_orders_order_id_unique
  ON execution_orders (order_id)
  WHERE order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS execution_fills (
  fill_key TEXT PRIMARY KEY,
  order_id TEXT NOT NULL,
  contract_key TEXT,
  fill_ts TIMESTAMPTZ,
  price NUMERIC(12, 6),
  quantity NUMERIC(18, 6),
  fee NUMERIC(18, 6),
  snapshot_observed_at TIMESTAMPTZ,
  snapshot_cohort_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_runtime_cycles_started_at
  ON runtime_cycles (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_decisions_cycle_id
  ON trade_decisions (cycle_id);

CREATE INDEX IF NOT EXISTS idx_execution_orders_cycle_id
  ON execution_orders (cycle_id);

CREATE INDEX IF NOT EXISTS idx_execution_orders_decision_id
  ON execution_orders (decision_id);

CREATE INDEX IF NOT EXISTS idx_execution_fills_order_id
  ON execution_fills (order_id);
