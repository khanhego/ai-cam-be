"""Stack đang chạy QA live — mặc định stack dev, stack QA riêng qua biến môi trường (T-229, DEC-820).

`AICAM_COMPOSE_PROJECT` (aicam-dev), `AICAM_COMPOSE_FILES` (docker/compose.dev.yml; nhiều file cách ":"), như
`scripts/qa-reset.sh`. Stack QA: `. docker/qa.env`.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT = os.environ.get("AICAM_COMPOSE_PROJECT", "aicam-dev")
FILES = os.environ.get("AICAM_COMPOSE_FILES", "docker/compose.dev.yml").split(":")
COMPOSE = ["docker", "compose", "-p", PROJECT, *[a for f in FILES for a in ("-f", str(ROOT / f))]]
