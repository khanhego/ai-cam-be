"""Lệnh `aicam` (02a §1).

- `aicam create-admin --username admin --display-name "Quản trị"`: tạo Admin đầu tiên (mật khẩu hỏi qua stdin
  hoặc biến `AICAM_ADMIN_PASSWORD`).
- `seed-demo` thêm ở T-9/T-10 khi có service đơn hàng và station.
"""

import argparse
import asyncio
import getpass
import os
import sys

from sqlalchemy import func, select

from aicam import __version__
from aicam.core.db import dispose_engine, init_engine, sessionmaker
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

    args = parser.parse_args(argv)
    if args.command == "create-admin":
        print(asyncio.run(create_admin(args.username, args.display_name, _read_password())))
        return 0
    parser.print_help(sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
