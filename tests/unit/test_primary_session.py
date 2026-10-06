"""BR-39 / DEC-448 (T-213): phiên chính, phiên mở hoàn trước — hàm thuần, không DB."""

import uuid
from datetime import UTC, datetime, timedelta

from aicam.modules.claims.evidence_rules import evidence_exclusion, is_prior_return, primary_session
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


# ---------------------------------------------------------------- BR-39 v0.3–v0.5 (T-279): luật loại phiên


def _cancelled(minutes: int, reason: str | None, cause: str | None = None, **kw: object) -> PackSession:
    s = _s("RETURN", "CANCELLED", minutes)
    s.cancel_reason, s.cancel_cause = reason, cause
    for k, v in kw.items():
        setattr(s, k, v)
    return s


def test_excluded_and_review_sessions_never_primary_even_if_earliest() -> None:
    pack, done = _s("PACK", "COMPLETED", -600), _s("RETURN", "COMPLETED", 30)
    station = _cancelled(0, "WRONG_SCAN")
    supervisor = _cancelled(1, "SUPERVISOR", "NOT_A_RETURN")
    marked = _s("RETURN", "ABANDONED", 2)
    marked.wrong_scan_at = T0
    review = _cancelled(3, "SUPERVISOR")  # Phase 2: không mã lý do → "Cần soát"
    everything = [pack, station, supervisor, marked, review, done]
    with_clip = {s.id for s in everything}

    assert primary_session(everything, with_clip, pack.id) == done.id
    assert primary_session([pack, station, supervisor, marked, review], with_clip, pack.id) == pack.id
    assert [evidence_exclusion(s) for s in (station, supervisor, marked, review)] == [
        "STATION_CANCEL", "SUPERVISOR_CANCEL", "MARKED", None,
    ]  # fmt: skip
    assert not is_prior_return(station, None)
    assert is_prior_return(review, None)  # cần soát vẫn là "phiên trước" (vào bằng chứng)


def test_confirmed_and_other_cause_are_regular() -> None:
    pack = _s("PACK", "COMPLETED", -600)
    confirmed = _cancelled(0, "WRONG_SCAN", review_confirmed_at=T0)
    other = _cancelled(5, "SUPERVISOR", "OTHER")
    assert primary_session([pack, confirmed, other], {confirmed.id, other.id}, pack.id) == confirmed.id
    # đã xác nhận rồi bị đánh dấu quét nhầm → loại lại (đánh dấu thắng xác nhận — BR-39 v0.5)
    confirmed.wrong_scan_at = T0
    assert primary_session([pack, confirmed, other], {confirmed.id, other.id}, pack.id) == other.id
