"""Mỗi tiến trình phải tự nạp đủ model (lỗi thật: `order.csv_import_id` không phân giải khi chạy CLI / api).

Chạy trong tiến trình Python mới để không bị model đã nạp sẵn trong phiên test che lỗi.
"""

import subprocess
import sys

import pytest

CHECK = """
import importlib, sys
importlib.import_module(sys.argv[1])
from aicam.core.db import Base
tables = Base.metadata.tables
for table in tables.values():
    for fk in table.foreign_keys:
        fk.column  # ném NoReferencedTableError nếu bảng đích chưa nạp
print(len(tables))
"""


@pytest.mark.parametrize(
    "module",
    ["aicam.main", "aicam.entrypoints.cli", "aicam.entrypoints.vision", "aicam.workers.tasks"],
)
def test_entrypoint_registers_all_tables(module: str) -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", CHECK, module], capture_output=True, text=True, check=False, timeout=60
    )

    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip() == "19"


def test_seed_demo_refuses_production(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review M1 #17: tài khoản TST mật khẩu chung không được tạo trên production."""
    from aicam.core.settings import Settings
    from aicam.entrypoints import cli

    prod = Settings.model_construct(app_env="production")
    monkeypatch.setattr(cli, "get_settings", lambda: prod)

    assert cli.main(["seed-demo"]) == 2
