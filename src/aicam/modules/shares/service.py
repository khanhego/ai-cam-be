"""API-160..164 — link chia sẻ bằng chứng (FR-07.05, 07.08, 07.09; BR-34, BR-35; 02a §4, §7.4).

An toàn bằng chứng / không lộ dữ liệu (NFR-42, NFR-45):
- Link chỉ chứa phiên **được chọn** của đúng nguồn: nguồn `CLAIM` → phiên trong `evidence[]` đang dùng
  (`removed_at IS NULL`, BR-38); nguồn `SESSION` → đúng phiên đó. Ảnh: chỉ ảnh `READY` của **phiên được chọn**
  (hồ sơ: ảnh trong `evidence[]`; phiên: ảnh của phiên) — không kéo ảnh của phiên không chọn (DEC-667).
- Phiên chọn phải có Cam 1 `READY` lúc tạo (409 `SESSION_CLIP_UNAVAILABLE`); J-24 kiểm lại lúc dựng.
- Phiên bị BR-39 loại (thêm tay) / "Cần soát" không chọn sẵn (DEC-668) — người dùng chọn tay mới vào.
- Danh sách phiên / ảnh chốt lúc tạo (`share_item`), J-24 không đọc lại bằng chứng.
- Token thư mục `share/{token_urlsafe(32)}/` (256 bit) không trả API, không log; URL Fernet (`url_enc`).
"""

import secrets
import uuid
from datetime import timedelta

import structlog
from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.claims import evidence_rules
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.claims.service import effective_pack_session
from aicam.modules.cloud import config as cloud
from aicam.modules.media import jobs as media_jobs
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.sessions.models import PackSession
from aicam.modules.shares import queries
from aicam.modules.shares.models import ShareItem, ShareLink
from aicam.modules.shares.schemas import (
    OptionLimits,
    OptionSession,
    ShareCounts,
    ShareCreated,
    ShareCreateIn,
    ShareError,
    ShareItemOut,
    ShareOptions,
    ShareOut,
    SharePage,
    ShareSource,
    SourceSha,
)
from aicam.modules.stations.models import Station

log = structlog.get_logger()

MAX_SESSIONS = 4
MAX_TOTAL_SECONDS = 1800
MAX_SNAPSHOTS = 20
EXPIRES_DAYS = (1, 3, 7)
DEFAULT_EXPIRES_DAYS = 7
RECIPIENT_MIN, RECIPIENT_MAX = 3, 100

BUILD_TASK = "shares.build"  # J-24, queue `export` (02a §7)
CLEANUP_TASK = "shares.cleanup"  # J-25, queue `default`


def new_object_prefix() -> str:
    """`share/{token}/` — token 256 bit ngẫu nhiên (`secrets`), base64url 43 ký tự (NFR-42 ≥ 128 bit)."""
    return f"share/{secrets.token_urlsafe(32)}/"


# ---------------------------------------------------------------- job (sau commit)


def enqueue_build(db: AsyncSession, share_id: uuid.UUID) -> None:
    async def _send() -> None:
        await media_jobs.send(BUILD_TASK, [str(share_id)], "export")

    after_commit(db, _send)


def enqueue_cleanup(db: AsyncSession, share_id: uuid.UUID | None = None) -> None:
    async def _send() -> None:
        await media_jobs.send(CLEANUP_TASK, [str(share_id)] if share_id else [], "default")

    after_commit(db, _send)


def publish_updated(db: AsyncSession, link: ShareLink) -> None:
    """WS-02 `share.updated` `{share_id, status, progress, step}` (ws:dashboard) sau commit."""
    data = {"share_id": str(link.id), "status": link.status, "progress": link.progress, "step": link.step}

    async def _send() -> None:
        from aicam.realtime import publish

        await publish.to_dashboard("share.updated", data)

    after_commit(db, _send)


# ---------------------------------------------------------------- nguồn + bằng chứng


class _Candidate:
    """Một phiên có thể đưa vào link (API-164 hàng, API-160 kiểm)."""

    def __init__(self, s: PackSession, station: str | None, clips: dict[str, Clip]) -> None:
        self.session = s
        self.station = station
        self.clips = clips
        self.primary = False
        self.prior_return = False
        self.snapshots: list[Snapshot] = []

    @property
    def cam1(self) -> Clip | None:
        return self.clips.get("CAM1")

    @property
    def selectable(self) -> bool:
        return self.cam1 is not None and self.cam1.status == "READY" and bool(self.cam1.path)

    @property
    def unavailable_reason(self) -> str | None:
        """EX-S3 + `MISSING` (02 API-164 v0.3): Cam 1 chưa `READY` → không chọn được."""
        return evidence_rules.cam1_unavailable_reason(self.cam1)

    @property
    def duration_s(self) -> int | None:
        cam1 = self.cam1
        if cam1 is None or cam1.duration_s is None:
            return None
        return round(float(cam1.duration_s))

    @property
    def cameras(self) -> list[str]:
        return [r for r in ("CAM1", "CAM2") if (c := self.clips.get(r)) is not None and c.status == "READY"]

    @property
    def held_back(self) -> bool:
        """BR-39: phiên bị loại (thêm tay) / cần soát — không vào link mặc định (DEC-668)."""
        return evidence_rules.excluded(self.session) or evidence_rules.review_needed(self.session)


class _Source:
    def __init__(self, source: ShareSource, claim: Claim | None, candidates: list[_Candidate]) -> None:
        self.source = source
        self.claim = claim
        self.candidates = candidates


async def _source_ref(
    db: AsyncSession, source_type: str, package: Package, claim: Claim | None
) -> ShareSource:
    order_id = (claim.order_id if claim else None) or package.order_id
    order = await db.get(Order, order_id) if order_id else None
    shop = await db.get(Shop, order.shop_id) if order and order.shop_id else None
    return ShareSource(
        type=source_type,
        claim_id=claim.id if claim else None,
        claim_code=claim.code if claim else None,
        package_id=package.id,
        tracking_number=package.tracking_number,
        platform=shop.platform if shop else None,
        shop_name=shop.name if shop else None,
    )


async def _candidates(
    db: AsyncSession, session_ids: list[uuid.UUID], *, lock: bool = False
) -> dict[uuid.UUID, _Candidate]:
    if not session_ids:
        return {}
    query = (
        select(PackSession, Station.name)
        .outerjoin(Station, Station.id == PackSession.station_id)
        .where(PackSession.id.in_(sorted(set(session_ids))))
    )
    if lock:
        # G3V-1 (DEC-932): API-160 nguồn PHIÊN — `FOR SHARE` phiên (API-189 MARK khóa `FOR UPDATE` rồi mới
        # truy `affected_shares`) + đọc lại bản mới → `held_back` kiểm sau khóa.
        query = query.with_for_update(read=True, of=PackSession).execution_options(populate_existing=True)
    rows = (await db.execute(query)).all()
    clips: dict[uuid.UUID, dict[str, Clip]] = {}
    for c in (await db.scalars(select(Clip).where(Clip.session_id.in_(sorted(set(session_ids)))))).all():
        clips.setdefault(c.session_id, {})[c.camera_role] = c
    return {s.id: _Candidate(s, name, clips.get(s.id, {})) for s, name in rows}


def _order(candidates: list[_Candidate]) -> list[_Candidate]:
    """Phiên chính trước, rồi theo `started_at` (02 API-164)."""
    return sorted(candidates, key=lambda c: (not c.primary, c.session.started_at, c.session.id))


async def _load_source(
    db: AsyncSession,
    source_type: str,
    claim_id: uuid.UUID | None,
    session_id: uuid.UUID | None,
    *,
    lock: bool = False,
) -> _Source:
    if source_type == "CLAIM":
        if claim_id is None:
            raise _invalid({"claim_id": "Thiếu hồ sơ."})
        query = select(Claim).where(Claim.id == claim_id)
        if lock:  # 02a API-160: đọc `evidence` nhất quán (API-134 / 189 khóa FOR UPDATE)
            query = query.with_for_update(read=True)
        claim = await db.scalar(query.execution_options(populate_existing=True))
        if claim is None:
            raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
        package = await db.get(Package, claim.package_id)
        if package is None:
            raise AppError("NOT_FOUND", "Không tìm thấy kiện hàng.", 404)
        rows = (
            await db.scalars(
                select(ClaimEvidence).where(
                    ClaimEvidence.claim_id == claim.id,
                    ClaimEvidence.removed_at.is_(None),  # BR-38
                )
            )
        ).all()
        session_ids = [e.session_id for e in rows if e.session_id]
        by_id = await _candidates(db, session_ids)
        sessions = [c.session for c in by_id.values()]
        with_clip = await evidence_rules.live_clip_sessions(db, by_id.keys())
        effective = await effective_pack_session(db, claim.package_id)
        primary = evidence_rules.primary_session(sessions, with_clip, effective.id if effective else None)
        latest = await evidence_rules.latest_completed_return_start(
            db, claim.package_id, claim.return_case_id
        )
        for c in by_id.values():
            c.primary = c.session.id == primary
            c.prior_return = c.session.id in with_clip and evidence_rules.is_prior_return(c.session, latest)
        snapshot_ids = [e.snapshot_id for e in rows if e.snapshot_id]
        if snapshot_ids:
            for snap in (
                await db.scalars(
                    select(Snapshot)
                    .where(Snapshot.id.in_(snapshot_ids), Snapshot.status == "READY")
                    .order_by(Snapshot.taken_at, Snapshot.id)
                )
            ).all():
                if snap.session_id in by_id:  # DEC-667: ảnh đi theo phiên của nó
                    by_id[snap.session_id].snapshots.append(snap)
        ref = await _source_ref(db, "CLAIM", package, claim)
        return _Source(ref, claim, _order(list(by_id.values())))
    if session_id is None:
        raise _invalid({"session_id": "Thiếu phiên."})
    by_id = await _candidates(db, [session_id], lock=lock)
    if session_id not in by_id:
        raise AppError("NOT_FOUND", "Không tìm thấy phiên.", 404)
    candidate = by_id[session_id]
    package = await db.get(Package, candidate.session.package_id)
    if package is None:
        raise AppError("NOT_FOUND", "Không tìm thấy kiện hàng.", 404)
    candidate.snapshots = list(
        (
            await db.scalars(
                select(Snapshot)
                .where(Snapshot.session_id == session_id, Snapshot.status == "READY")
                .order_by(Snapshot.taken_at, Snapshot.id)
            )
        ).all()
    )
    ref = await _source_ref(db, "SESSION", package, None)
    return _Source(ref, None, [candidate])


def _invalid(fields: dict[str, str]) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})


# ---------------------------------------------------------------- API-164


async def options(
    db: AsyncSession, claim_id: uuid.UUID | None, session_id: uuid.UUID | None, settings: Settings
) -> ShareOptions:
    if (claim_id is None) == (session_id is None):
        raise _invalid({"source": "Chọn đúng một nguồn: hồ sơ hoặc phiên."})
    src = await _load_source(db, "CLAIM" if claim_id else "SESSION", claim_id, session_id)
    sessions: list[OptionSession] = []
    picked, total = 0, 0
    for c in src.candidates:
        s = c.session
        default = False
        # BR-39 (DEC-668): phiên bị loại / "Cần soát" không chọn sẵn — cả nguồn PHIÊN (G3V-2, DEC-933: API-160
        # nguồn phiên sẽ 409 `SESSION_EXCLUDED`).
        if c.selectable and not c.held_back and picked < MAX_SESSIONS:
            duration = c.duration_s or 0
            if total + duration <= MAX_TOTAL_SECONDS:
                default, picked, total = True, picked + 1, total + duration
        reason = c.unavailable_reason
        sessions.append(
            OptionSession(
                id=s.id,
                type=s.type,
                status=s.status,
                started_at=s.started_at,
                ended_at=s.ended_at,
                station_name=c.station,
                operator_name=s.operator_name,
                conclusion=s.inspection_conclusion if s.type == "RETURN" else None,
                duration_s=c.duration_s,
                prior_return=c.prior_return,
                primary=c.primary,
                default_selected=default,
                selectable=c.selectable,
                unavailable_reason=reason,
                unavailable_at=c.cam1.deleted_at if reason == "CLIP_DELETED" and c.cam1 else None,
                cameras=c.cameras,
                review_needed=evidence_rules.review_needed(s),
                excluded=evidence_rules.excluded(s),
                evidence_exclusion=evidence_rules.evidence_exclusion(s),
                snapshot_count=len(c.snapshots),
            )
        )
    # G3-EV-4: phiên chính (BR-39 — lần mở hộp đầu) không dùng được Cam 1 → FE báo "Phiên chính thiếu tệp".
    primary_reason = next((c.unavailable_reason for c in src.candidates if c.primary), None)
    review_pending = 0
    if src.claim is not None:
        review_pending = len(
            await evidence_rules.review_sessions(db, src.claim.package_id, src.claim.return_case_id)
        )
    return ShareOptions(
        storage_configured=cloud.share_configured(settings),
        review_pending_count=review_pending,
        primary_unavailable=primary_reason is not None,
        primary_unavailable_reason=primary_reason,
        source=src.source,
        sessions=sessions,
        snapshot_count=sum(len(c.snapshots) for c in src.candidates if c.selectable),
        limits=OptionLimits(
            max_sessions=MAX_SESSIONS, max_total_seconds=MAX_TOTAL_SECONDS, max_snapshots=MAX_SNAPSHOTS
        ),
        default_expires_days=DEFAULT_EXPIRES_DAYS,
    )


# ---------------------------------------------------------------- API-160


def _validate(body: ShareCreateIn) -> tuple[list[uuid.UUID], str]:
    fields: dict[str, str] = {}
    ids = list(dict.fromkeys(body.session_ids))  # bỏ trùng, giữ thứ tự
    if not ids:
        fields["session_ids"] = "Chọn ít nhất 1 phiên."
    elif len(ids) > MAX_SESSIONS:
        fields["session_ids"] = "Chọn tối đa 4 phiên."
    recipient = " ".join(body.recipient.split())
    if not RECIPIENT_MIN <= len(recipient) <= RECIPIENT_MAX:
        fields["recipient"] = "Ghi rõ gửi cho ai (3–100 ký tự)."
    if body.expires_days not in EXPIRES_DAYS:
        fields["expires_days"] = "Hạn link chỉ 1, 3 hoặc 7 ngày."
    if body.source_type == "CLAIM" and body.claim_id is None:
        fields["claim_id"] = "Thiếu hồ sơ."
    if body.source_type == "SESSION" and body.session_id is None:
        fields["session_id"] = "Thiếu phiên."
    if fields:
        raise _invalid(fields)
    return ids, recipient


async def create(db: AsyncSession, body: ShareCreateIn, p: Principal, settings: Settings) -> ShareCreated:
    ids, recipient = _validate(body)
    src = await _load_source(db, body.source_type, body.claim_id, body.session_id, lock=True)
    by_id = {c.session.id: c for c in src.candidates}
    if any(sid not in by_id for sid in ids):
        raise _invalid({"session_ids": "Phiên không thuộc hồ sơ này."})
    if not cloud.share_configured(settings):
        raise AppError("CLOUD_NOT_CONFIGURED", "Chưa cấu hình kho lưu cloud. Admin: Cài đặt → Sao lưu.", 503)
    chosen = [c for c in src.candidates if c.session.id in set(ids)]  # thứ tự nguồn: phiên chính trước
    if src.claim is None:
        # G3-FE-1: nguồn PHIÊN (D4) — phiên RETURN bị loại theo BR-39 (đánh dấu quét nhầm / hủy WRONG_SCAN /
        # NOT_A_RETURN chưa xác nhận) hoặc "Cần soát" (`review_needed`) có thể là video kiện khác → không gửi
        # như bằng chứng của kiện này tới khi được xác nhận / gỡ đánh dấu (API-189). Nguồn HỒ SƠ: phiên loại
        # chỉ có trong `evidence[]` khi người dùng thêm tay có chủ đích (BR-39) — giữ nguyên.
        for c in chosen:
            if c.held_back:
                raise AppError(
                    "SESSION_EXCLUDED",
                    "Phiên mở hoàn này bị loại khỏi bằng chứng (quét nhầm / cần soát) — xác nhận ở hồ sơ "
                    "khiếu nại trước khi gửi link.",
                    409,
                    {"session_id": str(c.session.id)},
                )
    for c in chosen:
        if not c.selectable:
            raise AppError(
                "SESSION_CLIP_UNAVAILABLE",
                "Phiên vừa mất clip hoặc chưa có clip Cam 1 — tải lại danh sách phiên.",
                409,
                {"session_id": str(c.session.id), "reason": c.unavailable_reason},
            )
    if sum(c.duration_s or 0 for c in chosen) > MAX_TOTAL_SECONDS:
        raise _invalid({"session_ids": "Tổng thời lượng tối đa 30 phút."})
    budget = MAX_SNAPSHOTS if body.include_snapshots else 0
    now = clock.now()
    link = ShareLink(
        status="CREATING",
        source_type=body.source_type,
        claim_id=src.claim.id if src.claim else None,
        package_id=src.source.package_id,
        layout=body.layout,
        include_snapshots=body.include_snapshots,
        recipient=recipient,
        expires_at=now + timedelta(days=body.expires_days),
        object_prefix=new_object_prefix(),
        progress=0,
        step_total=len(chosen),
        created_by=p.user_id,
        created_at=now,
    )
    db.add(link)
    await db.flush()
    for order, c in enumerate(chosen, start=1):
        snaps = c.snapshots[:budget]
        budget -= len(snaps)
        db.add(
            ShareItem(
                share_id=link.id, ord=order, session_id=c.session.id, snapshot_ids=[s.id for s in snaps]
            )
        )
    audit.record(
        db,
        "SHARE_CREATE",
        user_id=p.user_id,
        object_type="SHARE",
        object_id=link.id,
        ip=p.ip,
        data={
            "recipient": recipient,
            "source_type": body.source_type,
            "claim_id": str(src.claim.id) if src.claim else None,
            "package_id": str(src.source.package_id),
            "session_ids": [str(c.session.id) for c in chosen],
            "session_count": len(chosen),
            "layout": body.layout,
            "include_snapshots": body.include_snapshots,
            "expires_days": body.expires_days,
            "expires_at": clock.iso_z(link.expires_at),
        },
    )  # không URL / token (NFR-42)
    enqueue_build(db, link.id)
    publish_updated(db, link)
    out = ShareCreated(id=link.id, status="CREATING")
    await commit(db)
    log.info("share_create", share_id=str(link.id), sessions=len(chosen), source=body.source_type)
    return out


# ---------------------------------------------------------------- API-161 / 162


async def _source_refs(db: AsyncSession, links: list[ShareLink]) -> dict[uuid.UUID, ShareSource]:
    if not links:
        return {}
    packages = {
        p.id: p
        for p in (
            await db.scalars(select(Package).where(Package.id.in_(sorted({x.package_id for x in links}))))
        ).all()
    }
    claim_ids = sorted({x.claim_id for x in links if x.claim_id})
    claims = (
        {c.id: c for c in (await db.scalars(select(Claim).where(Claim.id.in_(claim_ids)))).all()}
        if claim_ids
        else {}
    )
    order_ids = sorted(
        {
            oid
            for x in links
            for oid in [
                (claims[x.claim_id].order_id if x.claim_id in claims else None)
                or (packages[x.package_id].order_id if x.package_id in packages else None)
            ]
            if oid
        }
    )
    orders = (
        {o.id: o for o in (await db.scalars(select(Order).where(Order.id.in_(order_ids)))).all()}
        if order_ids
        else {}
    )
    shop_ids = sorted({o.shop_id for o in orders.values() if o.shop_id})
    shops = (
        {s.id: s for s in (await db.scalars(select(Shop).where(Shop.id.in_(shop_ids)))).all()}
        if shop_ids
        else {}
    )
    out: dict[uuid.UUID, ShareSource] = {}
    for x in links:
        package = packages[x.package_id]
        claim = claims.get(x.claim_id) if x.claim_id else None
        oid = (claim.order_id if claim else None) or package.order_id
        order = orders.get(oid) if oid else None
        shop = shops.get(order.shop_id) if order and order.shop_id else None
        out[x.id] = ShareSource(
            type=x.source_type,
            claim_id=claim.id if claim else x.claim_id,
            claim_code=claim.code if claim else None,
            package_id=package.id,
            tracking_number=package.tracking_number,
            platform=shop.platform if shop else None,
            shop_name=shop.name if shop else None,
        )
    return out


async def _outs(
    db: AsyncSession, links: list[ShareLink], p: Principal, settings: Settings, *, with_items: bool
) -> list[ShareOut]:
    now = clock.now()
    sources = await _source_refs(db, links)
    users = await queries.users_brief(db, [*(x.created_by for x in links), *(x.revoked_by for x in links)])
    items: dict[uuid.UUID, list[ShareItem]] = {}
    if links:
        for it in (
            await db.scalars(
                select(ShareItem).where(ShareItem.share_id.in_([x.id for x in links])).order_by(ShareItem.ord)
            )
        ).all():
            items.setdefault(it.share_id, []).append(it)
    out = []
    for x in links:
        rows = items.get(x.id, [])
        error = (
            ShareError(code=x.error_code, message=x.error_message or "")
            if x.status == "FAILED" and x.error_code
            else None
        )
        out.append(
            ShareOut(
                id=x.id,
                status=queries.effective_status(x, now),
                progress=x.progress,
                step=x.step if x.status == "CREATING" else None,
                step_index=x.step_index if x.status == "CREATING" else None,
                step_total=x.step_total if x.status == "CREATING" else None,
                url=queries.share_url(settings, x, now),
                recipient=x.recipient,
                source=sources[x.id],
                session_count=len(rows),
                layout=x.layout,
                include_snapshots=x.include_snapshots,
                expires_at=x.expires_at,
                created_at=x.created_at,
                created_by=users.get(x.created_by),
                revoked_at=x.revoked_at,
                revoked_by=users.get(x.revoked_by) if x.revoked_by else None,
                revoke_pending=queries.revoke_pending(x),
                error=error,
                items=[
                    ShareItemOut(
                        session_id=it.session_id,
                        order=it.ord,
                        video_sha256=it.video_sha256,
                        size_bytes=it.size_bytes,
                        source_sha256=SourceSha(**(it.source_sha256 or {})),
                        snapshot_count=len(it.snapshot_ids or []),
                    )
                    for it in rows
                ]
                if with_items
                else None,
                can_revoke=queries.can_revoke(x, p.user_id, p.role, now),
            )
        )
    return out


async def list_shares(
    db: AsyncSession,
    p: Principal,
    settings: Settings,
    *,
    status: str,
    q: str | None,
    mine: bool,
    created_by: uuid.UUID | None,
    claim_id: uuid.UUID | None,
    package_id: uuid.UUID | None,
    page: int,
    page_size: int,
) -> SharePage:
    now = clock.now()
    eff = queries.status_sql(now)
    conds: list[ColumnElement[bool]] = []
    if mine:
        conds.append(ShareLink.created_by == p.user_id)
    elif created_by is not None:
        conds.append(ShareLink.created_by == created_by)
    if claim_id is not None:
        conds.append(ShareLink.claim_id == claim_id)
    if package_id is not None:
        conds.append(ShareLink.package_id == package_id)
    if q and q.strip():
        term = q.strip()
        code = term.upper()
        conds.append(
            or_(
                ShareLink.package_id.in_(
                    select(Package.id).where(func.upper(Package.tracking_number) == code)
                ),
                ShareLink.claim_id.in_(select(Claim.id).where(func.upper(Claim.code) == code)),
                ShareLink.recipient.ilike(f"%{_like(term)}%", escape="\\"),
            )
        )
    by_status: dict[str, ColumnElement[bool]] = {
        "ACTIVE": eff.in_(queries.LIVE),
        "REVOKED": eff == "REVOKED",
        "EXPIRED": eff == "EXPIRED",
    }
    counts_row = (
        await db.execute(
            select(
                *(func.count().filter(cond).label(name) for name, cond in by_status.items()),
                func.count().label("ALL"),
            )
            .select_from(ShareLink)
            .where(*conds)
        )
    ).one()
    where = [*conds, *([by_status[status]] if status in by_status else [])]
    total = await db.scalar(select(func.count()).select_from(ShareLink).where(*where)) or 0
    links = list(
        (
            await db.scalars(
                select(ShareLink)
                .where(*where)
                .order_by(ShareLink.created_at.desc(), ShareLink.id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        ).all()
    )
    return SharePage(
        items=await _outs(db, links, p, settings, with_items=False),
        page=page,
        page_size=page_size,
        total=total,
        counts=ShareCounts(**counts_row._asdict()),
    )


def _like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def get_share(db: AsyncSession, share_id: uuid.UUID, p: Principal, settings: Settings) -> ShareOut:
    link = await db.get(ShareLink, share_id, populate_existing=True)
    if link is None:
        raise AppError("NOT_FOUND", "Không tìm thấy link chia sẻ.", 404)
    return (await _outs(db, [link], p, settings, with_items=True))[0]


# ---------------------------------------------------------------- API-163


async def revoke(db: AsyncSession, share_id: uuid.UUID, p: Principal, settings: Settings) -> ShareOut:
    link = await db.scalar(
        select(ShareLink)
        .where(ShareLink.id == share_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if link is None:
        raise AppError("NOT_FOUND", "Không tìm thấy link chia sẻ.", 404)
    if p.role not in queries.REVOKERS and link.created_by != p.user_id:
        raise AppError("FORBIDDEN", "Bạn chỉ thu hồi được link do mình tạo.", 403)
    if queries.effective_status(link) not in queries.LIVE:
        raise AppError("SHARE_NOT_ACTIVE", _not_active_message(queries.effective_status(link)), 409)
    now = clock.now()
    link.status = "REVOKED"
    link.revoked_at = now
    link.revoked_by = p.user_id
    link.step = None
    audit.record(
        db,
        "SHARE_REVOKE",
        user_id=p.user_id,
        object_type="SHARE",
        object_id=link.id,
        ip=p.ip,
        data={"recipient": link.recipient, "claim_id": str(link.claim_id) if link.claim_id else None,
              "package_id": str(link.package_id)},
    )  # fmt: skip
    await db.flush()
    enqueue_cleanup(db, link.id)  # J-25 xóa `share/{token}/` ngay (FR-07.08 ≤ 60 giây)
    publish_updated(db, link)
    out = (await _outs(db, [link], p, settings, with_items=True))[0]
    await commit(db)
    log.info("share_revoke", share_id=str(link.id))
    return out


def _not_active_message(status: str) -> str:
    return {
        "REVOKED": "Link đã được thu hồi.",
        "EXPIRED": "Link đã hết hạn.",
        "FAILED": "Link tạo không thành công — không cần thu hồi.",
    }.get(status, "Link không còn hoạt động.")
