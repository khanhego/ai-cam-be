"""Khử nhiễu tập mã trên khay (02a §7 Vision) — logic thuần để test được.

- Tập mã mới (≥ 1 mã) phải thấy giống nhau ≥ `confirm_frames` khung liên tiếp mới nhận.
- Khay trống (0 mã) phải ≥ `empty_frames` khung liên tiếp: tay che phiếu thoáng qua không làm mất mã.
- Mất stream quá `lost_after_s` giây → không còn trạng thái (`UNAVAILABLE`).
"""

from dataclasses import dataclass, field

CONFIRM_FRAMES = 2
EMPTY_FRAMES = 4
LOST_AFTER_S = 3.0


@dataclass
class TrayDebouncer:
    confirm_frames: int = CONFIRM_FRAMES
    empty_frames: int = EMPTY_FRAMES
    # None = chưa có trạng thái ổn định / mất stream (UNAVAILABLE).
    stable: tuple[str, ...] | None = None
    _candidate: tuple[str, ...] | None = field(default=None, repr=False)
    _count: int = field(default=0, repr=False)

    def observe(self, codes: tuple[str, ...]) -> bool:
        """Một khung đọc được (có thể 0 mã). Trả True khi trạng thái ổn định đổi."""
        if codes == self.stable:
            self._candidate, self._count = None, 0
            return False
        if codes == self._candidate:
            self._count += 1
        else:
            self._candidate, self._count = codes, 1
        needed = self.confirm_frames if codes else self.empty_frames
        if self._count < needed:
            return False
        self.stable = codes
        self._candidate, self._count = None, 0
        return True

    def lose(self) -> bool:
        """Mất stream. Trả True khi trước đó đang có trạng thái."""
        changed = self.stable is not None
        self.stable = None
        self._candidate, self._count = None, 0
        return changed
