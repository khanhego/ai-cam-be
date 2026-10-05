import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Role = Literal["ADMIN", "SUPERVISOR", "CSKH", "STATION"]
Client = Literal["STATION", "DASHBOARD"]


class StationRef(BaseModel):
    id: uuid.UUID
    name: str


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    username: str
    display_name: str
    role: Role
    station: StationRef | None = None


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=128)
    client: Client


class LoginOut(BaseModel):
    access_token: str
    expires_in: int
    user: UserOut


class RefreshIn(BaseModel):
    client: Client


class TokenOut(BaseModel):
    access_token: str
    expires_in: int


class MeOut(UserOut):
    permissions: list[str]


class UserListItem(UserOut):
    is_active: bool
    created_at: datetime


USERNAME_PATTERN = r"^[a-z0-9._-]{3,32}$"


class UserCreateIn(BaseModel):
    username: str = Field(pattern=USERNAME_PATTERN)
    display_name: str = Field(min_length=1, max_length=80)
    role: Role
    password: str = Field(min_length=8, max_length=128)


class UserPatchIn(BaseModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=80)
    role: Role | None = None
    is_active: bool | None = None
    password: str | None = Field(default=None, min_length=8, max_length=128)


class AuditUser(BaseModel):
    id: uuid.UUID
    display_name: str


class AuditLogOut(BaseModel):
    at: datetime
    user: AuditUser | None
    action: str
    object_type: str | None
    object_id: str | None
    data: dict[str, object] | None = None
