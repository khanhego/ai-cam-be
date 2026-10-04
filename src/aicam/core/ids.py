"""UUID v7 (RFC 9562) tăng đơn điệu: sắp theo id = sắp theo thứ tự tạo, kể cả trong cùng mili-giây (02a §3).

Dùng phương pháp 1 của RFC 9562 §6.2: 12 bit `rand_a` là bộ đếm, khởi tạo ngẫu nhiên mỗi mili-giây mới.
"""

import os
import threading
import time
import uuid

_lock = threading.Lock()
_last_ms = -1
_counter = 0
_COUNTER_MAX = 0x0FFF


def uuid7() -> uuid.UUID:
    global _last_ms, _counter
    with _lock:
        unix_ms = time.time_ns() // 1_000_000
        if unix_ms > _last_ms:
            _last_ms = unix_ms
            _counter = int.from_bytes(os.urandom(2), "big") & 0x07FF  # chừa một nửa không gian để tăng
        else:
            _counter += 1
            if _counter > _COUNTER_MAX:  # tràn trong cùng ms: mượn mili-giây kế tiếp
                _last_ms += 1
                _counter = 0
        ms, counter = _last_ms, _counter
    rand_b = int.from_bytes(os.urandom(8), "big") & 0x3FFF_FFFF_FFFF_FFFF
    value = (ms & 0xFFFF_FFFF_FFFF) << 80
    value |= 0x7 << 76
    value |= counter << 64
    value |= 0b10 << 62
    value |= rand_b
    return uuid.UUID(int=value)
