"""BR-41 công thức + kỳ báo cáo (02 §6 "Kỳ báo cáo", EX-B1, EX-B2) — ví dụ số của 01 BR-41 / AC-45..47."""

from datetime import UTC, date, datetime, timedelta

import pytest

from aicam.core import clock
from aicam.core.errors import AppError
from aicam.modules.reports import analytics as a

TZ = "Asia/Ho_Chi_Minh"


def test_br41_pack_average_minus_approval_wait() -> None:
    """3 phiên 60, 90, 150 giây (phiên 150 có 30 giây chờ duyệt) → TB 90 giây."""
    t0 = datetime(2026, 10, 1, 2, 0, tzinfo=UTC)
    durations = [
        a.net_seconds(t0, t0 + timedelta(seconds=60)),
        a.net_seconds(t0, t0 + timedelta(seconds=90)),
        a.net_seconds(t0, t0 + timedelta(seconds=150), [20.0, 10.0]),
    ]
    assert durations == [60.0, 90.0, 120.0]
    assert a.round_seconds(sum(durations), 3) == 90


def test_br41_rates_from_examples() -> None:
    assert a.ratio(25 + 15, 1000).value == 0.04  # 4,0 %
    assert a.rate(6, 1000) == 0.006  # Chỉ hoàn tiền 0,6 %
    assert a.ratio(6, 30).value == 0.2  # có vấn đề 20,0 %
    assert a.ratio(12, 12 + 4).value == 0.75  # tỷ lệ thắng 75 %
    assert a.ratio(14, 320).value == 0.0438


def test_zero_denominator_is_null() -> None:
    """EX-B2: mẫu số 0 → "—" (không chia cho 0)."""
    r = a.ratio(0, 0)
    assert (r.numerator, r.denominator, r.value) == (0, 0, None)
    assert a.round_seconds(0, 0) is None
    assert a.round_seconds(None, 3) is None


def test_round_seconds_half_up_and_never_negative() -> None:
    assert a.round_seconds(181, 2) == 91  # 90,5 → 91 (không làm tròn kiểu ngân hàng)
    t0 = datetime(2026, 10, 1, tzinfo=UTC)
    assert a.net_seconds(t0, t0 + timedelta(seconds=10), [30.0]) == 0.0


@pytest.fixture
def frozen() -> None:
    clock.freeze(datetime(2026, 10, 6, 2, 0, tzinfo=UTC))  # 09:00 VN ngày 06/10


@pytest.mark.usefixtures("frozen")
def test_period_validation_messages() -> None:
    def fields(from_: date | None, to: date | None) -> dict[str, str]:
        with pytest.raises(AppError) as exc:
            a.make_filters(from_, to, None, None, None, TZ)
        assert exc.value.status_code == 422
        return exc.value.details["fields"]  # type: ignore[no-any-return]

    assert fields(date(2026, 10, 5), date(2026, 10, 1)) == {"to": "Ngày đến phải sau ngày từ."}
    assert fields(date(2025, 10, 4), date(2026, 10, 5)) == {"from": "Chọn tối đa 366 ngày."}
    assert fields(date(2026, 10, 1), date(2026, 10, 7)) == {"to": "Không chọn ngày trong tương lai."}
    # 366 ngày đúng giới hạn, `to` = hôm nay (giờ VN) → hợp lệ
    ok = a.make_filters(date(2025, 10, 6), date(2026, 10, 6), "TIKTOK", None, None, TZ)
    assert ok.days == 366


@pytest.mark.usefixtures("frozen")
def test_period_default_30_days_to_today() -> None:
    f = a.make_filters(None, None, None, None, None, TZ)
    assert (f.from_, f.to, f.days) == (date(2026, 9, 7), date(2026, 10, 6), 30)


def test_bounds_are_vietnam_days() -> None:
    f = a.ReportFilters(date(2026, 9, 6), date(2026, 10, 5))
    start, end = a.bounds(f, TZ)
    assert start == datetime(2026, 9, 5, 17, 0, tzinfo=UTC)
    assert end == datetime(2026, 10, 5, 17, 0, tzinfo=UTC)


def test_cache_key_ignores_station_except_productivity() -> None:
    import uuid

    sid = uuid.uuid4()
    base = a.ReportFilters(date(2026, 9, 6), date(2026, 10, 5))
    with_station = a.ReportFilters(date(2026, 9, 6), date(2026, 10, 5), station_id=sid)
    assert a.cache_key("returns", base) == a.cache_key("returns", with_station)
    assert a.cache_key("productivity", base) != a.cache_key("productivity", with_station)
    assert a.cache_key("returns", base).startswith("report:returns:")
