"""T-228 (FR-10.02, FR-10.03): quyền API-04 Phase 3 theo 4 vai (02 §8 AuthZ, 04 TC-10.44) + action API-92
mới (02 §6.2 API-92) + mọi `audit.record(..., "ACTION")` trong code là action hợp lệ (bắt lỗi chính tả sớm).
"""

import ast
from pathlib import Path

import pytest

from aicam.core.audit import ACTIONS
from aicam.modules.users.permissions import PERMISSIONS

SRC = Path(__file__).resolve().parents[2] / "src" / "aicam"

PHASE3_PERMISSIONS = (
    "reports.returns",
    "reports.claims",
    "reports.productivity",
    "shares.create",
    "shares.read",
    "shares.revoke_any",
    "notify.manage",
    "backup.manage",
    "backup.read",
)
# 02 §8 AuthZ: ADMIN có tất cả; vai khác — đúng danh sách dưới (không thừa, không thiếu).
EXPECTED: dict[str, set[str]] = {
    "ADMIN": set(PHASE3_PERMISSIONS),
    "SUPERVISOR": {
        "reports.returns",
        "reports.claims",
        "reports.productivity",
        "shares.create",
        "shares.read",
        "shares.revoke_any",
        "backup.read",
    },
    "CSKH": {"reports.returns", "reports.claims", "shares.create", "shares.read"},
    "STATION": set(),
}

# 02 §6.2 API-92 (v0.4): action mới Phase 3.
PHASE3_ACTIONS = (
    "SHOP_DISCONNECT",
    "SHARE_CREATE",
    "SHARE_REVOKE",
    "SHARE_EXPIRE",
    "NOTIFY_CHANNEL_CREATE",
    "NOTIFY_CHANNEL_UPDATE",
    "NOTIFY_CHANNEL_DELETE",
    "NOTIFY_TEST",
    "NOTIFY_SETTINGS_UPDATE",
    "BACKUP_SETTINGS_UPDATE",
    "BACKUP_KEY_CONFIRM",
    "BACKUP_TEST",
    "BACKUP_RUN_NOW",
    "REPORT_EXPORT",
    "CLAIM_EVIDENCE_REMOVE",
    "BACKUP_REUPLOAD_OLD_KEY",
    "BACKUP_ISSUE_RESOLVE",
    "BACKUP_RESTORE_VERIFIED",
    "SESSION_WRONG_SCAN_MARK",
    "SESSION_WRONG_SCAN_UNMARK",
    "SESSION_RETURN_CONFIRM",
    "PACKAGE_CANCEL_REVERT",
    "BACKUP_VERIFY_ACCEPT",
    "MEDIA_MARK_MISSING",
    "MEDIA_MISSING_RECOVERED",
)


@pytest.mark.parametrize("role", sorted(EXPECTED))
def test_phase3_permissions_per_role(role: str) -> None:
    granted = set(PERMISSIONS[role]) & set(PHASE3_PERMISSIONS)
    assert granted == EXPECTED[role]
    assert len(PERMISSIONS[role]) == len(set(PERMISSIONS[role])), "quyền trùng"


def test_phase3_actions_registered() -> None:
    missing = [a for a in PHASE3_ACTIONS if a not in ACTIONS]
    assert not missing, f"API-92 thiếu action: {missing}"


def _audit_calls() -> list[tuple[str, int, str]]:
    """Mọi lời gọi `audit.record(db, "X", …)` / `record(db, "X", …)` có action là hằng chuỗi."""
    found: list[tuple[str, int, str]] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) < 2:
                continue
            func = node.func
            name = ""
            if isinstance(func, ast.Attribute):
                name = func.attr
            elif isinstance(func, ast.Name):
                name = func.id
            if name != "record":
                continue
            if isinstance(func, ast.Attribute) and not (
                isinstance(func.value, ast.Name) and func.value.id == "audit"
            ):
                continue
            arg = node.args[1]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                found.append((str(path.relative_to(SRC)), node.lineno, arg.value))
    return found


def test_every_audit_call_uses_known_action() -> None:
    calls = _audit_calls()
    assert len(calls) > 50  # quét thật sự thấy lời gọi
    unknown = [c for c in calls if c[2] not in ACTIONS]
    assert not unknown, unknown
    used = {c[2] for c in calls}
    # Action Phase 3 nào cũng có ít nhất một chỗ ghi trong code.
    never = [a for a in PHASE3_ACTIONS if a not in used]
    assert not never, f"action khai báo nhưng không chỗ nào ghi: {never}"
