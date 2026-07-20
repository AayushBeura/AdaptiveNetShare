"""
Centralised configuration for AdaptiveNetShare.

All constants that other modules depend on live here so there is a single
source of truth for ports, URLs, chunk sizes, etc.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Signalling server
# ---------------------------------------------------------------------------
SIGNALLING_HOST: str = "0.0.0.0"
SIGNALLING_PORT: int = int(os.environ.get("PORT", "8765"))
SIGNALLING_URL: str = os.environ.get(
    "ANS_SIGNALLING_URL", "wss://adaptivenetshare-signalling.onrender.com"
)

# ---------------------------------------------------------------------------
# ICE / STUN / TURN — Open Relay Project (free, no signup)
# https://www.metered.ca/tools/openrelay/
# ---------------------------------------------------------------------------
STUN_URLS: list[str] = [
    "stun:stun.l.google.com:19302",
    "stun:stun1.l.google.com:19302",
    "stun:openrelay.metered.ca:80",
]

TURN_URLS: list[str] = [
    "turn:openrelay.metered.ca:80",
    "turn:openrelay.metered.ca:443",
    "turn:openrelay.metered.ca:443?transport=tcp",
]

TURN_USERNAME: str = "openrelayproject"
TURN_CREDENTIAL: str = "openrelayproject"

# ---------------------------------------------------------------------------
# File transfer
# ---------------------------------------------------------------------------
CHUNK_SIZE: int = 65_536          # 64 KB per chunk
DATA_CHANNEL_LABEL: str = "file-transfer"
SLIDING_WINDOW_SIZE: int = 8      # chunks sent ahead without waiting for ACK

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DEFAULT_DOWNLOAD_DIR: Path = Path.home() / "Downloads" / "AdaptiveNetShare"
