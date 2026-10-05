"""Tạo dữ liệu test nhanh (tiền tố TST — 04-test-cases §1)."""

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.security import hash_password
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

PASSWORD = "matkhau123"
_HASH = hash_password(PASSWORD)


async def make_user(db: AsyncSession, username: str, role: str = "ADMIN", **kw: object) -> User:
    user = User(
        username=username, display_name=kw.pop("display_name", username), role=role, password_hash=_HASH, **kw
    )
    db.add(user)
    await db.flush()
    return user


async def make_station_account(
    db: AsyncSession, username: str = "tst_station01", name: str = "TST Station 01", active: bool = True
) -> tuple[User, Station]:
    user = await make_user(db, username, "STATION", display_name=name)
    station = Station(name=name, account_user_id=user.id, is_active=active)
    db.add(station)
    await db.flush()
    return user, station
