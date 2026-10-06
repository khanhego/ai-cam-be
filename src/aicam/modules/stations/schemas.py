import uuid
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator, model_validator

CameraRole = Literal["CAM1", "CAM2"]
StationKind = Literal["PACK", "RETURN", "BOTH"]
WorkMode = Literal["PACK", "RETURN"]


class Roi(BaseModel):
    x: float = Field(ge=0, le=1)
    y: float = Field(ge=0, le=1)
    w: float = Field(ge=0.05, le=1)
    h: float = Field(ge=0.05, le=1)

    @model_validator(mode="after")
    def _inside_frame(self) -> "Roi":
        if self.x + self.w > 1.0001 or self.y + self.h > 1.0001:
            raise ValueError("Vùng đọc mã vượt ra ngoài khung hình")
        return self


class AccountRef(BaseModel):
    id: uuid.UUID
    username: str


class CameraOut(BaseModel):
    id: uuid.UUID
    role: CameraRole
    rtsp_url_masked: str
    status: Literal["ONLINE", "OFFLINE"]
    roi: Roi | None
    clock_offset_ms: int | None = None


class StationOut(BaseModel):
    id: uuid.UUID
    name: str
    is_active: bool
    account: AccountRef | None
    cameras: list[CameraOut]
    # Phase 2 (02 §6.2 API-60).
    kind: StationKind
    work_mode: WorkMode
    operator_name: str | None


class StationList(BaseModel):
    items: list[StationOut]


class StationCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=40)
    account_user_id: uuid.UUID | None = None
    kind: StationKind = "PACK"


class StationPatchIn(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=40)
    is_active: bool | None = None
    account_user_id: uuid.UUID | None = None
    kind: StationKind | None = None


class CameraIn(BaseModel):
    rtsp_url: str = Field(pattern=r"^rtsp://", max_length=255)
    username: str | None = Field(default=None, max_length=64)
    password: str | None = Field(default=None, max_length=128)

    @field_validator("rtsp_url")
    @classmethod
    def _no_inline_credentials(cls, value: str) -> str:
        """Mật khẩu trong URL sẽ lưu không mã hóa và lộ ở log (review M1 #12)."""
        parts = urlsplit(value)
        if parts.username or parts.password:
            raise ValueError("Nhập tài khoản và mật khẩu camera ở ô riêng, không ghi trong địa chỉ")
        return value


class CameraTestOut(BaseModel):
    ok: bool
    snapshot: str
    clock_offset_ms: int | None


class LiveCamera(BaseModel):
    id: uuid.UUID
    role: CameraRole
    status: Literal["ONLINE", "OFFLINE"]
    whep_url: str


class LiveStation(BaseModel):
    id: uuid.UUID
    name: str
    cameras: list[LiveCamera]


class LiveOut(BaseModel):
    stations: list[LiveStation]
