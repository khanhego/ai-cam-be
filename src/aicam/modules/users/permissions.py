"""Quyền theo vai trò (01 §5.1, Phase 2 §5.10). FE chỉ dùng để ẩn/hiện; server kiểm bằng `require_roles`."""

# Phase 2 (02 §6.1 API-04, 01 §5.10).
_RETURNS_STAFF = ["returns.read", "recon.read", "claims.manage"]
_RETURNS_LEAD = ["returns.link", "inspection.correct", "recon.resolve", "warehouse_status.adjust"]
# Phase 3 link chia sẻ (02 §6.1 API-04, API-160..163): thu hồi link người khác chỉ ADMIN / SUPERVISOR.
_SHARES = ["shares.create", "shares.read"]
# Phase 3 báo cáo (02 §8 AuthZ, API-150..153): tab Năng suất chỉ ADMIN / SUPERVISOR (DEC-414).
_REPORTS = ["reports.returns", "reports.claims"]

PERMISSIONS: dict[str, list[str]] = {
    "ADMIN": [
        "station.manage",
        "camera.manage",
        "shop.manage",
        "settings.manage",
        "users.manage",
        "audit.read",
        "packages.read",
        "clips.read",
        "clips.export",
        "clips.hold",  # API-42 chỉ ADMIN (02 §6, G3 C-01)
        "clips.rebuild",
        "approvals.decide",
        "imports.write",
        "live.read",
        "reports.read",
        *_RETURNS_STAFF,
        *_RETURNS_LEAD,
        *_SHARES,
        "shares.revoke_any",
        *_REPORTS,
        "reports.productivity",
        "notify.manage",  # API-170..176 chỉ ADMIN (02 §8 AuthZ)
        "backup.manage",  # API-180..188 chỉ ADMIN
        "backup.read",
    ],
    "SUPERVISOR": [
        "packages.read",
        "clips.read",
        "clips.export",
        "clips.rebuild",
        "approvals.decide",
        "imports.write",
        "live.read",
        "reports.read",
        "settings.read",
        *_RETURNS_STAFF,
        *_RETURNS_LEAD,
        *_SHARES,
        "shares.revoke_any",
        *_REPORTS,
        "reports.productivity",
        # 02 §8: SUPERVISOR có `backup.read` nhưng API-180..188 chỉ ADMIN (02 §6.1) — quyền này chỉ để FE
        # hiện trạng thái sao lưu tóm tắt (API-81 / D2), không mở API sao lưu nào (DEC-780).
        "backup.read",
    ],
    # API-42 giữ clip chỉ ADMIN (02 §6, G3 C-01) — Phase 2 giữ theo hồ sơ khiếu nại.
    "CSKH": [
        "packages.read",
        "clips.read",
        "clips.export",
        "reports.read",
        *_RETURNS_STAFF,
        *_SHARES,
        *_REPORTS,
    ],
    "STATION": ["station.scan", "clips.read.own_station_today"],
}
