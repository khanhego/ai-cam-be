"""Lệnh `aicam` (02a §1).

- `aicam create-admin --username admin --display-name "Quản trị"`: tạo Admin đầu tiên (mật khẩu hỏi qua stdin
  hoặc biến `AICAM_ADMIN_PASSWORD`).
- `aicam seed-demo`: dữ liệu demo / test theo 04-test-cases §1 (tiền tố TST, mật khẩu `matkhau123`)
  + hàng hoàn mẫu (`seed_returns`, T-116). Chặn trên production.
- `aicam fix-cancel-requests [--apply]`: trả lại kiện hủy oan do Phase 2 coi yêu cầu hủy là hủy (T-285).
"""

import argparse
import asyncio
import getpass
import os
import sys

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam import __version__
from aicam.core import clock
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.redis import close_redis, init_redis
from aicam.core.security import hash_password
from aicam.core.settings import get_settings
from aicam.modules.users.models import User

MIN_PASSWORD = 8


async def create_admin(username: str, display_name: str, password: str) -> str:
    init_engine(get_settings().database_url)
    try:
        async with sessionmaker()() as session:
            exists = await session.scalar(
                select(User.id).where(func.lower(User.username) == username.lower())
            )
            if exists:
                return f"Tài khoản {username} đã tồn tại."
            session.add(
                User(
                    username=username.lower(),
                    display_name=display_name,
                    role="ADMIN",
                    password_hash=hash_password(password),
                )
            )
            await session.commit()
            return f"Đã tạo Admin {username}."
    finally:
        await dispose_engine()


DEMO_PASSWORD = "matkhau123"  # noqa: S105 — dữ liệu demo, in ra cho người dùng
DEMO_USERS = [
    ("tst_admin", "Quản trị", "ADMIN"),
    ("tst_sup", "Nguyễn B", "SUPERVISOR"),
    ("tst_cskh", "Lan", "CSKH"),
    ("tst_station01", "TST Station 01", "STATION"),
    ("tst_station02", "TST Station 02", "STATION"),
]


class SeedRefused(Exception):
    """seed-demo từ chối chạy (G3 F-12)."""


def seed_refusal(app_env: str, confirm_staging: bool) -> str | None:
    """G3 F-12 (DEC-343): chỉ dev / test; staging cần `--confirm-staging`; production không bao giờ."""
    if app_env in ("dev", "test"):
        return None
    if app_env == "staging" and confirm_staging:
        return None
    if app_env == "staging":
        return "seed-demo trên staging cần thêm --confirm-staging (tạo tài khoản TST mật khẩu chung)."
    return f"seed-demo không chạy trên {app_env}."


async def _foreign_users(session: AsyncSession) -> list[str]:
    """Tài khoản không phải `tst_*` (dữ liệu thật) → seed-demo không trộn dữ liệu giả vào (G3 F-12)."""
    rows = await session.scalars(
        select(User.username).where(~User.username.ilike("tst\\_%")).order_by(User.username).limit(5)
    )
    return list(rows.all())


async def seed_demo() -> list[str]:
    """Tạo dữ liệu demo idempotent (chạy lại không nhân đôi). DB có tài khoản không phải `tst_*` → từ chối."""
    from aicam.modules.orders import service as orders
    from aicam.modules.platforms.mock.adapter import MockAdapter
    from aicam.modules.sessions.models import PackSession
    from aicam.modules.stations import service as stations_service
    from aicam.modules.stations.mediamtx import HttpMediaMTX
    from aicam.modules.stations.models import Station
    from aicam.modules.stations.schemas import CameraIn

    from .seed_returns import seed_returns

    settings = get_settings()
    init_engine(settings.database_url)
    init_redis(settings.redis_url)  # J-14 (khóa `recon:run`) + publish WS sau commit
    lines: list[str] = []
    try:
        async with sessionmaker()() as session:
            foreign = await _foreign_users(session)
            if foreign:
                raise SeedRefused(
                    "seed-demo từ chối: DB đã có tài khoản không phải tst_* "
                    f"({', '.join(foreign)}) — không trộn dữ liệu demo vào dữ liệu thật."
                )
            users: dict[str, User] = {}
            for username, name, role in DEMO_USERS:
                user = await session.scalar(select(User).where(User.username == username))
                if user is None:
                    user = User(
                        username=username,
                        display_name=name,
                        role=role,
                        password_hash=hash_password(DEMO_PASSWORD),
                    )
                    session.add(user)
                    lines.append(f"+ tài khoản {username} ({role})")
                users[username] = user
            await session.flush()

            station_ids = {}
            for n in (1, 2):
                name = f"TST Station 0{n}"
                station = await session.scalar(select(Station).where(Station.name == name))
                if station is None:
                    station = Station(name=name, account_user_id=users[f"tst_station0{n}"].id)
                    session.add(station)
                    lines.append(f"+ {name}")
                station_ids[n] = station
            await session.flush()

            adapter = MockAdapter()
            for order in adapter.orders.values():
                await orders.upsert_platform_order(session, order)
            lines.append(f"= {len(adapter.orders)} đơn SPXTST0000001..30")

            for code, final in (("SPXTST0000010", "PACKED"), ("SPXTST0000011", "HANDED_OVER")):
                package = await orders.find_package(session, code)
                if package is None or package.warehouse_status != "NEW":
                    continue
                station = station_ids[2]
                now = clock.now()
                pack = PackSession(
                    package_id=package.id,
                    station_id=station.id,
                    status="COMPLETED",
                    started_at=now,
                    ended_at=now,
                    open_code=code,
                    close_code=code,
                    package_status_before="NEW",
                    flags=["CAM2_UNVERIFIED"],
                )
                session.add(pack)
                await orders.transition(
                    session, package, "PACKING", source="WAREHOUSE", actor_label=station.name
                )
                await orders.transition(
                    session, package, "PACKED", source="WAREHOUSE", actor_label=station.name
                )
                if final == "HANDED_OVER":
                    await orders.transition(
                        session, package, "HANDED_OVER", source="PLATFORM", actor_label="Sàn"
                    )
                lines.append(f"= {code} → {final}")
            await session.commit()

            # Camera cho Station 01 trỏ vào camera giả của stack dev (MediaMTX tự kéo từ path cam-fake*).
            mediamtx = HttpMediaMTX(settings.mediamtx_api_url)
            admin_id = users["tst_admin"].id
            for role, path in (("CAM1", "cam-fake1"), ("CAM2", "cam-fake2")):
                await stations_service.set_camera(
                    session, station_ids[1].id, role, CameraIn(rtsp_url=f"rtsp://localhost:8554/{path}"),
                    mediamtx=mediamtx, settings=settings, actor=admin_id, ip=None,
                )  # fmt: skip
            lines.append("= TST Station 01: Cam 1 → cam-fake1, Cam 2 → cam-fake2")

            # Phase 2 (T-116): hàng hoàn mẫu — đang về, chỉ hoàn tiền, chưa xác định, cảnh báo, khiếu nại.
            lines += await seed_returns(session, settings, station_ids[2], users["tst_sup"])
    finally:
        await close_redis()
        await dispose_engine()
    lines.append(f"Mật khẩu mọi tài khoản demo: {DEMO_PASSWORD}")
    return lines


async def fix_cancel_requests(apply: bool) -> int:
    """BR-21 v0.4 (DEC-519, T-285): trả lại kiện bị hủy oan do Phase 2 coi "yêu cầu hủy" là hủy."""
    from aicam.modules.orders.cancel_revert import fix_cancel_requests as run

    init_engine(get_settings().database_url)
    try:
        report = await run(sessionmaker(), apply=apply)
    finally:
        await dispose_engine()
    print("\n".join(report.lines))
    return 1 if report.failed else 0


def _read_password() -> str:
    password = os.environ.get("AICAM_ADMIN_PASSWORD") or getpass.getpass("Mật khẩu: ")
    if len(password) < MIN_PASSWORD:
        raise SystemExit(f"Mật khẩu phải có ít nhất {MIN_PASSWORD} ký tự.")
    return password


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aicam", description="Hệ thống X — công cụ quản trị")
    parser.add_argument("--version", action="version", version=f"aicam {__version__}")
    sub = parser.add_subparsers(dest="command")

    admin = sub.add_parser("create-admin", help="Tạo tài khoản Admin")
    admin.add_argument("--username", required=True)
    admin.add_argument("--display-name", default="Quản trị viên")

    seed = sub.add_parser("seed-demo", help="Tạo dữ liệu demo / test (TST…) — chỉ dev / test")
    seed.add_argument("--confirm-staging", action="store_true", help="cho phép chạy khi APP_ENV=staging")

    fix = sub.add_parser(
        "fix-cancel-requests",
        help="Trả lại kiện bị hủy oan do yêu cầu hủy (BR-21 v0.4) — mặc định chạy thử, --apply để ghi",
    )
    fix.add_argument("--apply", action="store_true", help="ghi thay đổi (không có = chỉ in danh sách)")

    args = parser.parse_args(argv)
    if args.command == "fix-cancel-requests":
        return asyncio.run(fix_cancel_requests(args.apply))
    if args.command == "create-admin":
        print(asyncio.run(create_admin(args.username, args.display_name, _read_password())))
        return 0
    if args.command == "seed-demo":
        # Tài khoản TST mật khẩu chung không được có trên production (review #17, G3 F-12).
        refusal = seed_refusal(get_settings().app_env, args.confirm_staging)
        if refusal:
            print(refusal, file=sys.stderr)
            return 2
        try:
            print("\n".join(asyncio.run(seed_demo())))
        except SeedRefused as exc:
            print(str(exc), file=sys.stderr)
            return 2
        return 0
    parser.print_help(sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
