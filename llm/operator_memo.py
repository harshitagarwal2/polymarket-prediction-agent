from __future__ import annotations

from execution.models import OrderProposal


def build_operator_memo(
    proposals: list[OrderProposal],
    *,
    blocked_reasons: list[str] | None = None,
) -> str:
    lines = ["Operator memo"]
    if proposals:
        lines.append(f"Proposals: {len(proposals)}")
        for proposal in proposals[:5]:
            lines.append(
                f"- {proposal.market_id} {proposal.side} {proposal.action} {proposal.size:.4f} @ {proposal.price:.4f}"
            )
    else:
        lines.append("Proposals: 0")
    if blocked_reasons:
        lines.append("Blocked reasons:")
        for reason in blocked_reasons[:5]:
            lines.append(f"- {reason}")
    return "\n".join(lines)
