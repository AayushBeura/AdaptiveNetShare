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
# ICE / STUN / TURN
# ---------------------------------------------------------------------------
STUN_URLS: list[str] = [
    "stun:stun.l.google.com:19302",
    "stun:stun1.l.google.com:19302",
]

METERED_API_URL: str = os.environ.get(
    "ANS_METERED_API_URL", 
    "https://adaptivenetshare.metered.live/api/v1/turn/credentials?apiKey=c7f992909e19de856c993a6b99c6a5fb3c66"
)

# ---------------------------------------------------------------------------
# File transfer
# ---------------------------------------------------------------------------
CHUNK_SIZE: int = 262_144          # 256 KB per chunk (larger = fewer round-trips)
DATA_CHANNEL_LABEL: str = "file-transfer"
SLIDING_WINDOW_SIZE: int = 64      # chunks sent ahead without waiting for ACK

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DEFAULT_DOWNLOAD_DIR: Path = Path.home() / "Downloads" / "AdaptiveNetShare"
