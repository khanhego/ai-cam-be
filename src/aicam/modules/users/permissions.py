"""Quyền theo vai trò (01 §5.1, Phase 2 §5.10). FE chỉ dùng để ẩn/hiện; server kiểm bằng `require_roles`."""

# Phase 2 (02 §6.1 API-04, 01 §5.10).
_RETURNS_STAFF = ["returns.read", "recon.read", "claims.manage"]
_RETURNS_LEAD = ["returns.link", "inspection.correct", "recon.resolve", "warehouse_status.adjust"]

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
    ],
    # API-42 giữ clip chỉ ADMIN (02 §6, G3 C-01) — Phase 2 giữ theo hồ sơ khiếu nại.
    "CSKH": ["packages.read", "clips.read", "clips.export", "reports.read", *_RETURNS_STAFF],
    "STATION": ["station.scan", "clips.read.own_station_today"],
}
