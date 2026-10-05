"""Xuất `openapi.json` (gốc repo) từ FastAPI cho FE sinh client; contract test so snapshot này với code.

Chạy: `uv run python scripts/export_openapi.py`
"""

import json
from pathlib import Path

from aicam.main import create_app

OUT = Path(__file__).resolve().parents[1] / "openapi.json"


def main() -> None:
    spec = create_app().openapi()
    OUT.write_text(json.dumps(spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Đã ghi {OUT} ({len(spec['paths'])} path)")


if __name__ == "__main__":
    main()
