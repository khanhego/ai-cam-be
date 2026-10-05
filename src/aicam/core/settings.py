"""Cấu hình đọc từ biến môi trường (02a §9)."""

from functools import lru_cache
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEV_SECRET_PREFIX = "dev-only-"  # noqa: S105 — tiền tố nhận diện secret dev, không phải secret


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "dev"  # dev | test | staging | production
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
    export_font_file: Path = Path("/usr/share/fonts/truetype/bevietnampro/BeVietnamPro-SemiBold.ttf")
    # Phase 2 (02a §9): sàn giữ clip (BR-25, DEC-210) — chỉ đổi qua env; migration 0003 nâng setting lên sàn.
    retention_clip_min_days: int = 60
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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
