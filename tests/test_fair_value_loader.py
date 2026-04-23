from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from adapters.types import Contract, MarketSummary, OutcomeSide, Venue
from engine.discovery import ManifestFairValueProvider, StaticFairValueProvider
from engine.fair_value_loader import (
    FairValueLookup,
    ReloadingFairValueProvider,
    build_fair_value_provider,
)
from research.fair_value_manifest import FairValueManifestBuild


def make_manifest_market(
    *,
    condition_id: str | None = "condition-1",
    event_key: str | None = "event-1",
    sport: str | None = "nba",
    series: str | None = "nba-finals",
    game_id: str | None = "game-1",
    sports_market_type: str | None = "moneyline",
) -> MarketSummary:
    raw = None
    if condition_id is not None:
        raw = {"market": {"condition_id": condition_id}}
    return MarketSummary(
        contract=Contract(
            venue=Venue.POLYMARKET,
            symbol="token-1",
            outcome=OutcomeSide.YES,
        ),
        event_key=event_key,
        sport=sport,
        series=series,
        game_id=game_id,
        sports_market_type=sports_market_type,
        raw=raw,
    )


class FairValueLoaderTests(unittest.TestCase):
    def test_build_fair_value_provider_supports_static_payloads(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump({"token-1:yes": 0.61}, handle)
            handle.flush()

            provider = build_fair_value_provider(handle.name)

        self.assertIsInstance(provider, StaticFairValueProvider)
        if not isinstance(provider, StaticFairValueProvider):
            self.fail("expected static fair value provider")
        self.assertEqual(provider.fair_value_for(make_manifest_market()), 0.61)

    def test_build_fair_value_provider_supports_manifest_records(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 1,
                    "generated_at": "2026-04-07T12:00:00Z",
                    "source": "sports-model-v1",
                    "max_age_seconds": 900,
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "condition_id": "condition-1",
                        }
                    },
                },
                handle,
            )
            handle.flush()

            provider = build_fair_value_provider(handle.name)

        self.assertIsInstance(provider, ManifestFairValueProvider)
        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")
        self.assertEqual(provider.max_age_seconds, 900.0)
        self.assertEqual(provider.source, "sports-model-v1")
        self.assertEqual(provider.records["token-1:yes"].fair_value, 0.61)
        self.assertEqual(provider.records["token-1:yes"].condition_id, "condition-1")

    def test_build_fair_value_provider_accepts_shared_manifest_builder_payload(self):
        generated_at = datetime.now(timezone.utc).replace(microsecond=0)
        manifest = FairValueManifestBuild(
            generated_at=generated_at,
            source="unit-test",
            max_age_seconds=900,
            values={
                "token-1:yes": {
                    "fair_value": 0.61,
                    "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
                    "condition_id": "condition-1",
                    "event_key": "event-1",
                }
            },
        )

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(manifest.to_payload(), handle)
            handle.flush()

            provider = build_fair_value_provider(handle.name)

        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")

        self.assertEqual(provider.source, "unit-test")
        self.assertEqual(provider.fair_value_for(make_manifest_market()), 0.61)

    def test_build_fair_value_provider_rejects_unknown_manifest_schema_version(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 2,
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "condition_id": "condition-1",
                        }
                    },
                },
                handle,
            )
            handle.flush()

            with self.assertRaisesRegex(
                RuntimeError,
                "unsupported fair-value manifest schema_version",
            ):
                build_fair_value_provider(handle.name)

    def test_build_fair_value_provider_requires_identity_for_versioned_manifest_records(
        self,
    ):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 1,
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                        }
                    },
                },
                handle,
            )
            handle.flush()

            with self.assertRaisesRegex(
                RuntimeError,
                "manifest record must include event identity",
            ):
                build_fair_value_provider(handle.name)

    def test_build_fair_value_provider_supports_calibrated_field_selection(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "calibrated_fair_value": 0.67,
                            "condition_id": "condition-1",
                            "event_key": "event-1",
                        }
                    },
                },
                handle,
            )
            handle.flush()

            provider = build_fair_value_provider(
                handle.name,
                fair_value_field="calibrated",
            )

        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")

        self.assertEqual(provider.fair_value_field, "calibrated")
        self.assertEqual(provider.records["token-1:yes"].calibrated_fair_value, 0.67)
        self.assertEqual(provider.fair_value_for(make_manifest_market()), 0.67)

    def test_build_fair_value_provider_uses_manifest_identity_and_generated_at_fallback(
        self,
    ):
        generated_at = datetime.now(timezone.utc).replace(microsecond=0)

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
                    "max_age_seconds": 900,
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "condition_id": "condition-1",
                            "event_key": "event-1",
                        }
                    },
                },
                handle,
            )
            handle.flush()

            provider = build_fair_value_provider(handle.name)

        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")

        self.assertEqual(provider.fair_value_for(make_manifest_market()), 0.61)
        self.assertIsNone(
            provider.fair_value_for(make_manifest_market(condition_id="condition-2"))
        )
        self.assertIsNone(
            provider.fair_value_for(make_manifest_market(event_key="event-2"))
        )
        self.assertIsNone(
            provider.fair_value_for(make_manifest_market(condition_id=None))
        )

    def test_build_fair_value_provider_uses_extended_market_identity_fields(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "condition_id": "condition-1",
                            "event_key": "event-1",
                            "sport": "nba",
                            "series": "nba-finals",
                            "game_id": "game-1",
                            "sports_market_type": "moneyline",
                        }
                    },
                },
                handle,
            )
            handle.flush()

            provider = build_fair_value_provider(handle.name)

        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")

        self.assertEqual(provider.fair_value_for(make_manifest_market()), 0.61)
        self.assertIsNone(provider.fair_value_for(make_manifest_market(sport="nfl")))
        self.assertIsNone(
            provider.fair_value_for(make_manifest_market(series="western-conference"))
        )
        self.assertIsNone(
            provider.fair_value_for(make_manifest_market(game_id="game-2"))
        )
        self.assertIsNone(
            provider.fair_value_for(
                make_manifest_market(sports_market_type="championship_winner")
            )
        )

    def test_build_fair_value_provider_prefers_record_timestamp_for_staleness(self):
        manifest_generated_at = datetime.now(timezone.utc).replace(microsecond=0)
        stale_generated_at = manifest_generated_at - timedelta(hours=2)

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "generated_at": manifest_generated_at.isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "max_age_seconds": 60,
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "generated_at": stale_generated_at.isoformat().replace(
                                "+00:00", "Z"
                            ),
                            "condition_id": "condition-1",
                            "event_key": "event-1",
                        }
                    },
                },
                handle,
            )
            handle.flush()

            provider = build_fair_value_provider(handle.name)

        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")

        self.assertIsNone(provider.fair_value_for(make_manifest_market()))

    def test_reloading_fair_value_provider_reloads_after_interval(self):
        class Provider(FairValueLookup):
            def __init__(self, value):
                self.value = value

            def fair_value_for(self, market: object) -> float | None:
                return self.value

        values = iter([Provider(0.6), Provider(0.7), Provider(0.8)])
        provider = ReloadingFairValueProvider(
            lambda: next(values),
            reload_interval_seconds=0.0,
        )

        first = provider.fair_value_for(SimpleNamespace())
        second = provider.fair_value_for(SimpleNamespace())

        self.assertEqual(first, 0.7)
        self.assertEqual(second, 0.8)


if __name__ == "__main__":
    unittest.main()
