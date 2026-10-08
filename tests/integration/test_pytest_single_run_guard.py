"""BUG-G4-2 (DEC-971): hai tiến trình pytest cùng dùng DB test (`aicam_test`, `aicam_test_mig`, Redis
db 15) làm test phụ thuộc dữ liệu toàn cục chập chờn (J-02 / `check_timeouts` đếm mọi phiên; migration dựng
lại schema `_mig` của bên kia). Phiên pytest giữ khóa tệp theo DB test; tiến trình thứ hai dừng ngay với
lời nhắn rõ."""

import subprocess
import sys

import pytest

from .conftest import acquire_single_run_lock, single_run_lock_path

pytestmark = pytest.mark.integration


def test_second_pytest_process_cannot_take_test_db_lock(migrated_database_url: str) -> None:
    path = single_run_lock_path(migrated_database_url)
    probe = (
        "import fcntl, sys\n"
        f"fh = open({str(path)!r}, 'a')\n"
        "try:\n"
        "    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
        "except BlockingIOError:\n"
        "    sys.exit(7)\n"
        "sys.exit(0)\n"
    )
    out = subprocess.run([sys.executable, "-c", probe], check=False, timeout=30)  # noqa: S603
    assert out.returncode == 7  # phiên pytest này đang giữ khóa


def test_same_process_reacquire_is_allowed(migrated_database_url: str) -> None:
    """`tests/contract/conftest.py` nhập lại fixture của integration (`import *`) → hai fixture phiên cùng một
    tiến trình: lần lấy thứ hai phải nhận ra khóa của **chính tiến trình này** (PID trong tệp), không dừng."""
    assert acquire_single_run_lock(single_run_lock_path(migrated_database_url)) is None
