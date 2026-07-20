"""
Protocol messages for the chunk-based file transfer.

Every message exchanged over the WebRTC data channel is one of the
dataclasses defined here, serialized with msgpack for compactness.

Public helpers:
    serialize(msg)   → bytes   (msgpack)
    deserialize(raw) → message dataclass instance
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional, Union

import msgpack


# ---------------------------------------------------------------------------
# Message dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Manifest:
    """Sent first to describe the incoming file."""
    transfer_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    filename: str = ""
    file_size: int = 0
    total_chunks: int = 0
    chunk_size: int = 0
    sha256: str = ""                       # whole-file hash
    type: str = field(default="manifest", init=False)


@dataclass
class ChunkMessage:
    """One chunk of file data."""
    transfer_id: str = ""
    chunk_index: int = 0
    total_chunks: int = 0
    sha256: str = ""                       # per-chunk hash
    data: bytes = b""
    type: str = field(default="chunk", init=False)


@dataclass
class AckMessage:
    """Positive acknowledgement for a received chunk or manifest."""
    transfer_id: str = ""
    chunk_index: int = -1                  # -1 means manifest ACK
    type: str = field(default="ack", init=False)


@dataclass
class NackMessage:
    """Negative acknowledgement — chunk hash mismatch or other error."""
    transfer_id: str = ""
    chunk_index: int = 0
    reason: str = ""
    type: str = field(default="nack", init=False)


@dataclass
class DoneMessage:
    """Sent by the receiver when all chunks are verified and assembled."""
    transfer_id: str = ""
    type: str = field(default="done", init=False)


# ---------------------------------------------------------------------------
# Type alias for any protocol message
# ---------------------------------------------------------------------------
Message = Union[Manifest, ChunkMessage, AckMessage, NackMessage, DoneMessage]

_TYPE_MAP = {
    "manifest": Manifest,
    "chunk": ChunkMessage,
    "ack": AckMessage,
    "nack": NackMessage,
    "done": DoneMessage,
}


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def serialize(msg: Message) -> bytes:
    """Serialize a message dataclass to msgpack bytes."""
    return msgpack.packb(asdict(msg), use_bin_type=True)


def deserialize(raw: bytes) -> Message:
    """Deserialize msgpack bytes back into the appropriate message dataclass."""
    data = msgpack.unpackb(raw, raw=False)
    msg_type = data.get("type")

    cls = _TYPE_MAP.get(msg_type)
    if cls is None:
        raise ValueError(f"Unknown message type: {msg_type!r}")

    # Pop 'type' since it's set by __post_init__ / field default
    data.pop("type", None)
    return cls(**data)
