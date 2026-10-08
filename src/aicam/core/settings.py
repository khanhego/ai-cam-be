"""Cấu hình đọc từ biến môi trường (02a §9)."""

import base64
import binascii
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEV_SECRET_PREFIX = "dev-only-"  # noqa: S105 — tiền tố nhận diện secret dev, không phải secret


# Trường secret (02a §9): không vào DB / log / repr (NFR-41, T-228).
SECRET_FIELDS = (
    "jwt_secret",
    "fernet_key",
    "media_signing_key",
    "shopee_partner_key",
    "tiktok_app_secret",
    "s3_secret_access_key",
    "backup_encryption_key",
    "telegram_bot_token",
    "zalo_app_secret",
    "zalo_oa_refresh_token",
)


class Settings(BaseSettings):
    # `hide_input_in_errors`: lỗi validator in cả dict đầu vào (gồm bot token, khóa sao lưu…) ra log khởi động
    # container — T-228 che log (NFR-41, DEC-781).
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True
    )

    app_env: Literal["dev", "test", "staging", "production"] = "dev"  # G3 F-12: giá trị lạ → lỗi khởi động
    # G3 M-F1 (DEC-336): schema DB lệch head của image → thoát (None = thoát ở staging / production;
    # dev / test chỉ log).
    schema_check_strict: bool | None = None
    log_level: str = "INFO"
    log_json: bool = True
    tz_display: str = "Asia/Ho_Chi_Minh"
    cors_origins: list[str] = []

    database_url: str = "postgresql+asyncpg://aicam:aicam@localhost:55432/aicam"
    redis_url: str = "redis://localhost:56379/0"

    # Secret: bản dev có giá trị mặc định; production bắt buộc đặt (kiểm ở validator).
    # `repr=False`: `repr(settings)` / log ngoại lệ không in secret (T-228, DEC-781).
    jwt_secret: str = Field(default=f"{_DEV_SECRET_PREFIX}jwt-secret-change-me-0123456789abcdef", repr=False)
    # base64 của 32 byte, chỉ dev
    fernet_key: str = Field(default="ZGV2LW9ubHktZmVybmV0LWtleS0zMmJ5dGVzLWxvbmc=", repr=False)
    media_signing_key: str = Field(default=f"{_DEV_SECRET_PREFIX}media-signing-key", repr=False)

    access_token_minutes: int = 15
    refresh_days_dashboard: int = 7
    refresh_days_station: int = 30
    login_max_fails: int = 10
    login_lock_minutes: int = 15
    login_ip_max_fails: int = 30  # 429 theo IP trong 5 phút (DEC-45)
    cookie_secure: bool = True  # localhost vẫn nhận cookie Secure

    video_root: Path = Path("/data/video")
    mediamtx_api_url: str = "http://localhost:59997"
    mediamtx_rtsp_url: str = "rtsp://localhost:58554"

    scan_code_regex: str = r"^[A-Z0-9-]{8,40}$"
    # Phase 2 (02a §9): mã đơn sàn chấp nhận khi quét ở bàn hoàn; API-104 tìm tiền tố từ N ký tự.
    order_sn_regex: str = r"^[A-Z0-9]{10,20}$"
    return_lookup_prefix_min: int = 6
    platform_lookup_timeout_s: float = 2.0
    clip_padding_s: int = 5

    # Media (T-14, T-15; spike S3 — 02a mục Spike S3).
    ffmpeg_bin: str = "ffmpeg"
    ffprobe_bin: str = "ffprobe"
    media_url_ttl_s: int = 600  # URL ký HMAC hạn 10 phút (02 §8)
    clip_settle_s: float = 3.0  # chờ thêm sau `ended_at + đệm` để MediaMTX ghi xong phần cuối (spike S3)
    clip_gap_tolerance_s: float = 1.5  # khe hở lớn hơn → VIDEO_INCOMPLETE
    clip_cut_timeout_s: int = 90
    segment_closed_after_s: float = 15.0  # file không đổi quá lâu = segment đã đóng (camera ngừng)
    export_preset: str = "veryfast"  # DEC-101: hạ "superfast" nếu server kho encode > 20 giây (AC-08)
    export_side_scale: str = "1280:720"  # kích thước mỗi camera trong bản xuất; dự phòng "960:540"
    export_ttl_hours: int = 24  # 02a API-44/45: file xuất giữ ≤ 24 giờ (J-10 dọn)
    export_timeout_s: int = 600
    # Phase 2 (02a §9): ảnh Cam 1 (API-103, J-17).
    snapshot_timeout_s: float = 3.0
    snapshot_max_per_session: int = 20
    snapshot_jpeg_quality: int = 85
    # T-121 (DEC-320): khung mới nhất do vision giữ trong Redis — tuổi tối đa khi dùng, nhịp ghi, bật / tắt.
    snapshot_frame_max_age_s: float = 2.0
    vision_frame_interval_s: float = 1.0
    vision_frames_enabled: bool = True
    export_font_file: Path = Path("/usr/share/fonts/truetype/bevietnampro/BeVietnamPro-SemiBold.ttf")
    # Phase 2 (02a §9): gói bằng chứng hồ sơ khiếu nại (J-16, API-136..138), dọn ở J-10.
    evidence_pack_ttl_hours: int = 24
    evidence_pack_timeout_s: int = 900
    # Phase 2 (02a §9): sàn giữ clip (BR-25, DEC-210) — chỉ đổi qua env; migration 0003 nâng setting lên sàn.
    retention_clip_min_days: int = 60
    # Phase 2 (02a §9): đối soát J-14 — tắt khi sự cố; giới hạn mềm mỗi lần chạy.
    recon_enabled: bool = True
    recon_run_soft_limit_s: int = 120
    import_root: Path = Path("/data/imports")  # file CSV / xlsx gốc (T-17); J-11 xóa sau 90 ngày

    platform_adapter: str = "mock"  # shopee | mock
    shopee_enabled: bool = False
    shopee_partner_id: str = ""
    shopee_partner_key: str = Field(default="", repr=False)
    shopee_redirect_url: str = ""
    shopee_base_url: str = "https://partner.shopeemobile.com"
    # T-16 / T-22 (02a §7, FR-05.08). Chưa đo với Shopee thật (thiếu tài khoản partner — T-3).
    shopee_timeout_s: float = 10.0  # mỗi request HTTP
    shopee_max_attempts: int = 5  # thử lại giãn cách mũ 0,5 / 1 / 2 / 4 giây (hoặc theo Retry-After)
    shopee_backoff_s: float = 0.5
    shopee_lookup_lookback_min: int = 60  # tra khi quét: dò đơn cập nhật trong 60 phút gần nhất
    shopee_initial_sync_days: int = 3  # lần đồng bộ đầu sau khi kết nối
    # Phase 2 J-13 (02a §9) — chờ T-3 xác nhận giới hạn thật của `returns.get_return_list`.
    shopee_returns_page_size: int = 50
    shopee_returns_window_days: int = 15
    # G3 F-11 (DEC-342): J-13 tắt riêng tới khi T-3 xác nhận API `returns` thật (adapter mock luôn chạy).
    shopee_returns_enabled: bool = False
    # G3 F-10: lượt J-13 đầu (shop chưa có cursor) lùi N ngày — shop kết nối từ Phase 1 cần lùi xa hơn 3 ngày.
    shopee_returns_initial_days: int = 15

    # Phase 3 — TikTok Shop (02a §9, FR-05.20, EX-T1). Giả định theo tài liệu công khai — chưa có
    # tài khoản đối tác (Q18, Q19). `TIKTOK_ADAPTER=mock` chỉ dev / test (validator cấm khi bật ở production).
    tiktok_enabled: bool = False
    tiktok_returns_enabled: bool = False
    tiktok_adapter: str = "mock"  # mock | tiktok
    tiktok_app_key: str = ""
    tiktok_app_secret: str = Field(default="", repr=False)
    tiktok_service_id: str = ""
    tiktok_api_base: str = "https://open-api.tiktokglobalshop.com"
    tiktok_auth_base: str = "https://auth.tiktok-shops.com"
    tiktok_authorize_url: str = "https://services.tiktokshop.com/open/authorize"
    tiktok_redirect_url: str = ""  # rỗng = {SITE_ADDRESS}/api/v1/shops/tiktok/callback (RK-26)
    tiktok_timeout_s: float = 10.0
    tiktok_max_attempts: int = 5
    tiktok_backoff_s: float = 0.5
    tiktok_lookup_lookback_min: int = 60
    tiktok_initial_sync_days: int = 3
    tiktok_returns_initial_days: int = 15
    mock_shopee_shop_ids: str = "990001,990002"  # 02a §7.2 — mock Shopee nhiều shop (dev / test)
    # Ngân sách mỗi task một shop (02a §7, §9): J-04 (queue sync_fast) / J-06, J-13 (queue sync).
    sync_task_budget_s: float = 120.0
    sync_long_task_budget_s: float = 300.0

    # Phase 3 — kho lưu cloud S3-compatible (02a §9, ADR-010, DEC-501). Rỗng → sao lưu + link
    # `NOT_CONFIGURED`.
    # Chưa có nhà cung cấp thật (Q20): dev / test dùng MinIO (compose dev) hoặc `MemoryStore`.
    s3_endpoint: str = ""
    s3_region: str = "us-east-1"
    s3_bucket: str = ""  # bucket sao lưu (versioning + lifecycle — DEC-501)
    s3_share_bucket: str = ""  # bucket link chia sẻ (không versioning)
    s3_addressing_style: Literal["path", "virtual", "auto"] = "path"
    s3_access_key_id: str = ""
    s3_secret_access_key: str = Field(default="", repr=False)
    s3_public_endpoint: str = ""  # rỗng = S3_ENDPOINT (host ký URL link)
    share_build_timeout_s: int = 600  # J-24 dựng + tải link (02a §9); hết → `FAILED TIMEOUT`
    # Sao lưu (02a §9). Khóa base64 32 byte — không bao giờ vào DB / log / bản sao (FR-02.13, NFR-41).
    backup_encryption_key: str = Field(default="", repr=False)
    backup_old_keys: str = Field(
        default="", repr=False
    )  # khóa cũ (base64, cách dấu phẩy) chỉ để giải mã — DEC-495
    backup_tmp_dir: Path = Path("/tmp/aicam-backup")  # noqa: S108 — thư mục tạm của container worker-backup
    backup_db_budget_s: int = 3600
    backup_upload_budget_s: int = 240
    backup_source_missing_mark_after: int = 4  # DEC-530 (T-291)
    # `pg_dump` / `pg_restore` (image: postgresql-client-16). Máy dev không cài → trỏ wrapper chạy container.
    backup_pg_dump_bin: str = "pg_dump"
    backup_pg_restore_bin: str = "pg_restore"

    # Phase 3 — thông báo Telegram / Zalo OA (02a §7.5, §9; FR-06.04..06.11). Bot token / khóa OA chỉ ở biến
    # môi trường (DEC-408); token Zalo xoay vòng lưu DB mã hóa Fernet (DEC-445). Chưa có bot / OA thật (Q21):
    # dev / test dùng `NOTIFY_TRANSPORT=mock` (ghi Redis `notify:mock:{type}` + log).
    notify_enabled: bool = True
    notify_transport: Literal["real", "mock"] = "real"
    notify_mock_fail: str = ""  # dev / test: loại kênh luôn lỗi, cách dấu phẩy (vd `TELEGRAM`)
    telegram_bot_token: str = Field(default="", repr=False)
    telegram_api_base: str = "https://api.telegram.org"
    zalo_app_id: str = ""
    zalo_app_secret: str = Field(default="", repr=False)
    zalo_oa_refresh_token: str = Field(default="", repr=False)
    zalo_api_base: str = "https://openapi.zalo.me"  # giả định theo tài liệu công khai (Q21)
    zalo_oauth_base: str = "https://oauth.zaloapp.com"
    # Dashboard trong tin "Xem: https://{SITE_ADDRESS}/admin/…" (AS-16). Rỗng → tin không có dòng link.
    site_address: str = ""

    def secret_values(self) -> list[str]:
        """Giá trị secret đang đặt — bộ che log thay mọi lần xuất hiện bằng `[secret đã che]` (DEC-781)."""
        values = [getattr(self, name) for name in SECRET_FIELDS]
        values += [k.strip() for k in self.backup_old_keys.split(",")]
        return [v for v in values if v and len(v.strip()) >= 8]

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def fake_clock_allowed(self) -> bool:
        return self.app_env == "test"

    @model_validator(mode="after")
    def _require_real_secrets_in_production(self) -> "Settings":
        # Mọi môi trường dùng chung được (staging, production) đều cần secret thật (review M1 #17).
        if self.app_env not in ("dev", "test"):
            dev_values = [
                name
                for name in ("jwt_secret", "media_signing_key")
                if getattr(self, name).startswith(_DEV_SECRET_PREFIX)
            ]
            if self.fernet_key == Settings.model_fields["fernet_key"].default:
                dev_values.append("fernet_key")
            if dev_values:
                raise ValueError(
                    f"Môi trường {self.app_env} cần đặt secret thật cho: {', '.join(dev_values)}"
                )
        if self.platform_adapter not in ("shopee", "mock"):
            raise ValueError("PLATFORM_ADAPTER phải là shopee hoặc mock")
        if self.is_production and self.platform_adapter == "mock":
            # Adapter mock trả đơn giả cho mọi mã quét → kiện "đã xác minh" sai (G3-N13).
            raise ValueError("Production không được dùng PLATFORM_ADAPTER=mock")
        if self.tiktok_adapter not in ("tiktok", "mock"):
            raise ValueError("TIKTOK_ADAPTER phải là tiktok hoặc mock")
        if self.app_env not in ("dev", "test") and self.tiktok_enabled:
            # 02a §9: bật TikTok ở staging / production cần đủ khóa ứng dụng đối tác + adapter thật.
            missing = [
                name
                for name in ("tiktok_app_key", "tiktok_app_secret", "tiktok_service_id")
                if not getattr(self, name).strip()
            ]
            if missing:
                raise ValueError(f"TIKTOK_ENABLED cần đặt: {', '.join(m.upper() for m in missing)}")
            if self.tiktok_adapter == "mock":
                raise ValueError(
                    f"Môi trường {self.app_env} không được dùng TIKTOK_ADAPTER=mock khi bật TikTok"
                )
        self._validate_cloud()
        if self.is_production and self.notify_transport == "mock":
            # 02a §9: mock nuốt mọi tin (chỉ ghi Redis) — production mất cảnh báo mà không ai biết.
            raise ValueError("Production không được dùng NOTIFY_TRANSPORT=mock")
        return self

    def _validate_cloud(self) -> None:
        """02a §9: khóa sao lưu hợp lệ ở mọi môi trường (sai khóa = bản sao vô dụng — RK-19); production /
        staging có `S3_ENDPOINT` → đủ khóa truy cập, **hai** bucket khác nhau, không trỏ localhost."""
        for name, value in (("BACKUP_ENCRYPTION_KEY", self.backup_encryption_key),):
            if value.strip() and not _is_key(value):
                raise ValueError(f"{name} phải là base64 của đúng 32 byte (`aicam backup-keygen`)")
        old = [k.strip() for k in self.backup_old_keys.split(",") if k.strip()]
        for k in old:
            if not _is_key(k):
                raise ValueError("Mỗi khóa trong BACKUP_OLD_KEYS phải là base64 của đúng 32 byte")
            if self.backup_encryption_key.strip() and _key_bytes(k) == _key_bytes(self.backup_encryption_key):
                raise ValueError("BACKUP_OLD_KEYS không được chứa khóa hiện tại BACKUP_ENCRYPTION_KEY")
        if self.app_env in ("dev", "test") or not self.s3_endpoint.strip():
            return
        missing = [
            n.upper()
            for n in ("s3_access_key_id", "s3_secret_access_key", "s3_bucket", "s3_share_bucket")
            if not getattr(self, n).strip()
        ]
        if missing:
            raise ValueError(f"S3_ENDPOINT cần đặt: {', '.join(missing)}")
        if self.s3_bucket.strip() == self.s3_share_bucket.strip():
            raise ValueError("S3_BUCKET (sao lưu) và S3_SHARE_BUCKET (link) phải là hai bucket khác nhau")
        if self.is_production:
            for n in ("s3_endpoint", "s3_public_endpoint"):
                host = getattr(self, n).lower()
                if "localhost" in host or "127.0.0.1" in host:
                    raise ValueError(f"Production không được dùng {n.upper()} trỏ localhost")
            # NFR-42: link chia sẻ chỉ HTTPS — URL ký W1 đi qua Internet tới người ngoài.
            public = (self.s3_public_endpoint.strip() or self.s3_endpoint.strip()).lower()
            if not public.startswith("https://"):
                raise ValueError(
                    "Production: S3_PUBLIC_ENDPOINT (hoặc S3_ENDPOINT) phải là https:// (NFR-42)"
                )


def _key_bytes(value: str) -> bytes:
    return base64.b64decode(value.strip(), validate=True)


def _is_key(value: str) -> bool:
    try:
        return len(_key_bytes(value)) == 32  # AES-256
    except (binascii.Error, ValueError):
        return False


@lru_cache
def get_settings() -> Settings:
    return Settings()
