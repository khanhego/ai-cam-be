"""BR-39 / DEC-448 (T-213): phiên chính, phiên mở hoàn trước — hàm thuần, không DB."""

import uuid
from datetime import UTC, datetime, timedelta

from aicam.modules.claims.evidence_rules import is_prior_return, primary_session
from aicam.modules.sessions.models import PackSession

T0 = datetime(2026, 10, 6, 1, 0, tzinfo=UTC)


def _s(kind: str, status: str, minutes: int) -> PackSession:
    return PackSession(id=uuid.uuid4(), type=kind, status=status, started_at=T0 + timedelta(minutes=minutes))


def test_primary_is_earliest_return_with_clip() -> None:
    pack, a, b, c = (
        _s("PACK", "COMPLETED", -600),
        _s("RETURN", "ABANDONED", 0),
        _s("RETURN", "COMPLETED", 80),
        (_s("RETURN", "CANCELLED", -10)),
    )
    with_clip = {a.id, b.id}  # c không có clip

    assert primary_session([pack, a, b, c], with_clip, pack.id) == a.id
    assert primary_session([pack, a, b, c], with_clip, pack.id, excluded={a.id}) == b.id


def test_primary_falls_back_to_effective_pack_in_evidence() -> None:
    pack, c = _s("PACK", "COMPLETED", -600), _s("RETURN", "CANCELLED", 0)
    assert primary_session([pack, c], set(), pack.id) == pack.id
    assert primary_session([c], set(), pack.id) is None  # phiên đóng gói không trong bằng chứng
    assert primary_session([], set(), None) is None


def test_prior_return_relative_to_latest_completed() -> None:
    a, late = _s("RETURN", "ABANDONED", 0), _s("RETURN", "CANCELLED", 90)
    done_at = T0 + timedelta(minutes=80)
    assert is_prior_return(a, done_at)
    assert not is_prior_return(late, done_at)
    assert is_prior_return(late, None)  # chưa có phiên hoàn tất
    assert not is_prior_return(_s("RETURN", "COMPLETED", 0), None)
    assert not is_prior_return(_s("PACK", "CANCELLED", 0), None)
