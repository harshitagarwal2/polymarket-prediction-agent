from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import build_event_map_from_schedule_feed


class BuildEventMapFromScheduleFeedTests(unittest.TestCase):
    def test_file_provider_builds_event_map(self):
        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as schedule,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output,
        ):
            json.dump(
                {
                    "games": [
                        {
                            "source_event_id": "feed-1",
                            "home_team": "Home Team",
                            "away_team": "Away Team",
                            "start_time": "2026-04-21T20:00:00Z",
                            "status": "scheduled",
                        }
                    ]
                },
                schedule,
            )
            schedule.flush()

            stdout = io.StringIO()
            with patch("sys.stdout", stdout):
                result = build_event_map_from_schedule_feed.main(
                    [
                        "--provider",
                        "file",
                        "--schedule-file",
                        schedule.name,
                        "--output",
                        output.name,
                        "--sport",
                        "nba",
                        "--series",
                        "playoffs",
                    ]
                )

            payload = json.loads(Path(output.name).read_text())
            summary = json.loads(stdout.getvalue())

        self.assertEqual(result, 0)
        self.assertIn("feed-1", payload)
        self.assertEqual(payload["feed-1"]["sport"], "nba")
        self.assertEqual(payload["feed-1"]["series"], "playoffs")
        self.assertEqual(summary["mapped_event_count"], 1)

    def test_mlb_provider_requires_date(self):
        with self.assertRaisesRegex(RuntimeError, "--date is required"):
            build_event_map_from_schedule_feed.main(
                [
                    "--provider",
                    "mlb-statsapi",
                    "--output",
                    "runtime/odds_event_map.json",
                ]
            )


if __name__ == "__main__":
    unittest.main()
