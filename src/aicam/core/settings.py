"""Cấu hình đọc từ biến môi trường (02a §9)."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEV_SECRET_PREFIX = "dev-only-"  # noqa: S105 — tiền tố nhận diện secret dev, không phải secret


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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
    jwt_secret: str = f"{_DEV_SECRET_PREFIX}jwt-secret-change-me-0123456789abcdef"
    fernet_key: str = "ZGV2LW9ubHktZmVybmV0LWtleS0zMmJ5dGVzLWxvbmc="  # base64 của 32 byte, chỉ dev
    media_signing_key: str = f"{_DEV_SECRET_PREFIX}media-signing-key"

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
    shopee_partner_key: str = ""
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
    tiktok_app_secret: str = ""
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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
