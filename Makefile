.PHONY: sync sync-research reproduce

UV ?= uv

sync:
	$(UV) sync --locked

sync-research:
	$(UV) sync --locked --extra research

reproduce:
	$(UV) run --locked --extra research prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
