"""UUID v7 (RFC 9562): sắp xếp theo thời gian, sinh ở app (02a §3)."""

import os
import time
import uuid


def uuid7() -> uuid.UUID:
    unix_ms = time.time_ns() // 1_000_000
    rand = int.from_bytes(os.urandom(10), "big")
    value = (unix_ms & 0xFFFF_FFFF_FFFF) << 80
    value |= 0x7 << 76  # version
    value |= ((rand >> 62) & 0x0FFF) << 64  # rand_a, 12 bit
    value |= 0b10 << 62  # variant
    value |= rand & 0x3FFF_FFFF_FFFF_FFFF  # rand_b, 62 bit
    return uuid.UUID(int=value)
