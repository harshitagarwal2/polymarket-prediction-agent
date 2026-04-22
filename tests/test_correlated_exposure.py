from __future__ import annotations

import unittest

from risk.correlated_exposure import (
    CorrelatedExposureGraph,
    CorrelatedExposureLimit,
    exposure_by_market,
)


class CorrelatedExposureGraphTests(unittest.TestCase):
    def test_grouped_cluster_exposure_nets_mutually_exclusive_markets(self):
        graph = CorrelatedExposureGraph()
        graph.register_market(
            "winner-yes",
            cluster_key="event:event-1",
            mutually_exclusive_group_key="winner",
        )
        graph.register_market(
            "winner-no",
            cluster_key="event:event-1",
            mutually_exclusive_group_key="winner",
        )
        graph.register_market(
            "spread-home",
            cluster_key="event:event-1",
            mutually_exclusive_group_key="spread",
        )

        exposure = graph.grouped_cluster_exposure(
            cluster_key="event:event-1",
            exposure_by_market=exposure_by_market(
                [
                    ("winner-yes", 1.0),
                    ("winner-no", 0.8),
                    ("spread-home", 1.5),
                ]
            ),
        )
        self.assertEqual(exposure, 2.5)

    def test_cluster_limit_uses_projected_group_maximum(self):
        graph = CorrelatedExposureGraph()
        graph.register_market(
            "winner-yes",
            cluster_key="event:event-1",
            mutually_exclusive_group_key="winner",
        )
        graph.register_market(
            "winner-no",
            cluster_key="event:event-1",
            mutually_exclusive_group_key="winner",
        )

        decision = graph.cluster_exposure_ok(
            market_key="winner-no",
            exposure_by_market={"winner-yes": 1.0, "winner-no": 0.5},
            proposed_exposure=0.4,
            limit=CorrelatedExposureLimit(
                cluster_key="event:event-1",
                max_exposure=1.0,
            ),
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.projected_cluster_exposure, 1.0)
