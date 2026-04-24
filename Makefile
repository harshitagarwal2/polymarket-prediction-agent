.PHONY: sync sync-dev sync-research lint format format-check typecheck test test-contracts coverage audit smoke-compose smoke-service-stack check reproduce

UV ?= uv

sync:
	$(UV) sync --locked

sync-dev:
	$(UV) sync --locked --extra dev

sync-research:
	$(UV) sync --locked --extra research

lint:
	$(UV) run --locked --extra dev ruff check .

format:
	$(UV) run --locked --extra dev ruff format scripts/ingest_live_data.py tests/test_console_script_entrypoints.py tests/test_docs_sync.py tests/test_repository_hardening.py

format-check:
	$(UV) run --locked --extra dev ruff format --check scripts/ingest_live_data.py tests/test_console_script_entrypoints.py tests/test_docs_sync.py tests/test_repository_hardening.py

smoke-service-stack:
	bash -eu -o pipefail -c 'cleanup=0; if [ -z "$${PREDICTION_MARKET_POSTGRES_DSN:-}" ]; then cleanup=1; trap "if [ $$cleanup -eq 1 ]; then docker compose down -v; fi" EXIT; docker compose up -d postgres; POSTGRES_ID=$$(docker compose ps -q postgres); until [ "$$(docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" "$$POSTGRES_ID")" = "healthy" ]; do sleep 1; done; export PREDICTION_MARKET_POSTGRES_DSN=postgresql://prediction_market:prediction_market@localhost:5432/prediction_market; fi; $(UV) run --locked --extra postgres --extra polymarket python -m scripts.run_service_stack_smoke --root runtime/data --quiet'

typecheck:
	$(UV) run --locked --extra dev mypy

test:
	$(UV) run --locked python -m unittest discover -s tests -p "test_*.py"

test-contracts:
	$(UV) run --locked python -m unittest discover -s tests -p "test_llm_advisory.py"
	$(UV) run --locked python -m unittest discover -s tests -p "test_operator_advisory_cli.py"
	$(UV) run --locked python -m unittest discover -s tests -p "test_docs_sync.py"
	$(UV) run --locked python -m unittest discover -s tests -p "test_fair_value_loader.py"
	$(UV) run --locked python -m unittest discover -s tests -p "test_repository_hardening.py"

coverage:
	$(UV) run --locked --extra dev coverage run -m unittest discover -s tests -p "test_*.py"
	$(UV) run --locked --extra dev coverage report

audit:
	$(UV) sync --locked --extra dev --extra research --extra postgres --extra polymarket --extra kalshi
	$(UV) run --locked --extra dev pip-audit

smoke-compose:
	bash -eu -o pipefail -c 'trap "docker compose down -v" EXIT; docker compose config >/dev/null; docker compose up -d postgres; docker compose run --rm bootstrap-postgres; docker compose --profile projection run --rm run-current-projection'

check:
	$(UV) lock --check
	$(MAKE) lint
	$(MAKE) format-check
	$(MAKE) typecheck
	$(MAKE) test-contracts

reproduce:
	$(UV) run --locked --extra research prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
