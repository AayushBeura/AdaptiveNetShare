"""
SHA-256 integrity utilities for AdaptiveNetShare.

All hashing is done with Python's built-in hashlib — no external deps.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

_BUF_SIZE = 128 * 1024   # 128 KB read buffer for streaming


def hash_bytes(data: bytes) -> str:
    """Return the hex SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def hash_file(path: Path | str) -> str:
    """
    Stream-hash a file and return the hex SHA-256 digest.

    Reads in 128 KB chunks so even multi-GB files use constant memory.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(_BUF_SIZE)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def verify_chunk(data: bytes, expected_sha256: str) -> bool:
    """Return True if *data* hashes to *expected_sha256*."""
    return hash_bytes(data) == expected_sha256


def verify_file(path: Path | str, expected_sha256: str) -> bool:
    """Return True if the file at *path* hashes to *expected_sha256*."""
    return hash_file(path) == expected_sha256
