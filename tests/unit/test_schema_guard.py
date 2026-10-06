"""G3 M-F1 (DEC-336): head đóng gói trong image khớp thư mục migration."""

from alembic.script import ScriptDirectory

from aicam.core import schema_guard
from aicam.core.settings import Settings
from tests.integration.conftest import alembic_config


def test_schema_head_matches_alembic_versions() -> None:
    assert ScriptDirectory.from_config(alembic_config()).get_heads() == [schema_guard.SCHEMA_HEAD]


def test_strict_by_environment() -> None:
    prod = Settings(
        app_env="production",
        jwt_secret="x" * 40,
        media_signing_key="y" * 40,
        fernet_key="Zm9vYmFyYmF6cXV4cXV1eGNvcmdlZ3JhdWx0Z2FycGx5PQ==",
        platform_adapter="shopee",
    )
    assert schema_guard.is_strict(prod)
    assert not schema_guard.is_strict(Settings(app_env="dev"))
    assert schema_guard.is_strict(Settings(app_env="dev", schema_check_strict=True))
