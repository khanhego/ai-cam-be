"""Chống CSV / formula injection (G3-RP-1, OWASP "CSV Injection").

Ô **chuỗi** bắt đầu `=` `+` `-` `@` tab hoặc CR được Excel / Google Sheets coi là công thức → thêm `'` đầu ô.
Số (int / float) và chuỗi dạng số ("-1,5", "4,0%") giữ nguyên để cột số vẫn là số; "—" không đổi.
"""

import csv
import re
from collections.abc import Iterable, Sequence
from typing import Any

_TRIGGERS = ("=", "+", "-", "@", "\t", "\r")
_NUMERIC = re.compile(r"^[+-]?\d[\d.,]*%?$")


def safe_cell(value: Any) -> Any:
    if isinstance(value, str) and value.startswith(_TRIGGERS) and not _NUMERIC.match(value):
        return "'" + value
    return value


def safe_row(row: Iterable[Any]) -> list[Any]:
    return [safe_cell(v) for v in row]


class SafeWriter:
    """`csv.writer` bọc `safe_cell` cho mọi ô."""

    def __init__(self, fh: Any, **kwargs: Any) -> None:
        self._writer = csv.writer(fh, **kwargs)

    def writerow(self, row: Iterable[Any]) -> None:
        self._writer.writerow(safe_row(row))

    def writerows(self, rows: Iterable[Sequence[Any]]) -> None:
        for row in rows:
            self.writerow(row)
