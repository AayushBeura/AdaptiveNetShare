"""
File splitting (sender) and reassembly (receiver).

FileSender  — reads a local file and produces Manifest + ChunkMessages.
FileReceiver — accepts chunks, verifies integrity, writes to a temp file,
               and renames to the final filename on completion.
"""

from __future__ import annotations

import math
import os
import uuid
import logging
from pathlib import Path
from typing import Set

from adaptivenetshare.config import CHUNK_SIZE, DEFAULT_DOWNLOAD_DIR
from adaptivenetshare.core.integrity import hash_bytes, hash_file
from adaptivenetshare.core.messages import (
    Manifest,
    ChunkMessage,
)

logger = logging.getLogger(__name__)


class FileSender:
    """Reads a file and produces protocol messages for the transfer."""

    def __init__(self, file_path: Path | str, chunk_size: int = CHUNK_SIZE) -> None:
        self.path = Path(file_path)
        if not self.path.is_file():
            raise FileNotFoundError(f"Not a file: {self.path}")

        self.chunk_size = chunk_size
        self.file_size = self.path.stat().st_size
        self.total_chunks = max(1, math.ceil(self.file_size / self.chunk_size))
        self.transfer_id = str(uuid.uuid4())

        logger.info(
            "FileSender: %s  (%d bytes, %d chunks of %d)",
            self.path.name, self.file_size, self.total_chunks, self.chunk_size,
        )

        # Compute whole-file hash (streams, constant memory)
        self.file_sha256 = hash_file(self.path)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def get_manifest(self) -> Manifest:
        """Return a Manifest message describing the file."""
        return Manifest(
            transfer_id=self.transfer_id,
            filename=self.path.name,
            file_size=self.file_size,
            total_chunks=self.total_chunks,
            chunk_size=self.chunk_size,
            sha256=self.file_sha256,
        )

    def get_chunk(self, index: int) -> ChunkMessage:
        """
        Read chunk *index* from the file and return a ChunkMessage.

        Raises IndexError if index is out of range.
        """
        if index < 0 or index >= self.total_chunks:
            raise IndexError(
                f"Chunk index {index} out of range [0, {self.total_chunks})"
            )

        offset = index * self.chunk_size
        with open(self.path, "rb") as f:
            f.seek(offset)
            data = f.read(self.chunk_size)

        return ChunkMessage(
            transfer_id=self.transfer_id,
            chunk_index=index,
            total_chunks=self.total_chunks,
            sha256=hash_bytes(data),
            data=data,
        )


class FileReceiver:
    """Accepts chunks, verifies hashes, and assembles the final file."""

    def __init__(
        self,
        manifest: Manifest,
        download_dir: Path | str | None = None,
    ) -> None:
        self.manifest = manifest
        self.download_dir = Path(download_dir) if download_dir else DEFAULT_DOWNLOAD_DIR
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Temp file lives next to the final location
        self._final_path = self.download_dir / manifest.filename
        self._temp_path = self._final_path.with_suffix(
            self._final_path.suffix + ".part"
        )

        # Pre-allocate the temp file so we can write chunks at arbitrary offsets
        with open(self._temp_path, "wb") as f:
            f.truncate(manifest.file_size)

        self._received: Set[int] = set()

        logger.info(
            "FileReceiver: expecting %s  (%d bytes, %d chunks)",
            manifest.filename, manifest.file_size, manifest.total_chunks,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def receive_chunk(self, chunk: ChunkMessage) -> bool:
        """
        Verify and write a single chunk.

        Returns True (ACK) on success, False (NACK) on hash mismatch.
        """
        # Verify per-chunk hash
        actual_hash = hash_bytes(chunk.data)
        if actual_hash != chunk.sha256:
            logger.warning(
                "Chunk %d hash mismatch: expected %s, got %s",
                chunk.chunk_index, chunk.sha256, actual_hash,
            )
            return False

        # Write to the correct offset in the temp file
        offset = chunk.chunk_index * self.manifest.chunk_size
        with open(self._temp_path, "r+b") as f:
            f.seek(offset)
            f.write(chunk.data)

        self._received.add(chunk.chunk_index)
        logger.debug(
            "Chunk %d/%d received OK",
            chunk.chunk_index + 1, self.manifest.total_chunks,
        )
        return True

    @property
    def received_indices(self) -> Set[int]:
        """Return the set of chunk indices that have been successfully received."""
        return set(self._received)

    def is_complete(self) -> bool:
        """Return True if every chunk has been received."""
        return len(self._received) == self.manifest.total_chunks

    def finalize(self) -> Path:
        """
        Rename temp file to final filename.

        Raises RuntimeError if not all chunks have been received.
        Returns the final file path.
        """
        if not self.is_complete():
            missing = self.manifest.total_chunks - len(self._received)
            raise RuntimeError(
                f"Cannot finalize: still missing {missing} chunks"
            )

        # Handle name collision by appending a counter
        final_path = self._final_path
        if final_path.exists():
            stem = final_path.stem
            suffix = final_path.suffix
            counter = 1
            while final_path.exists():
                final_path = self.download_dir / f"{stem} ({counter}){suffix}"
                counter += 1

        os.replace(self._temp_path, final_path)
        logger.info("File saved: %s", final_path)
        return final_path

    def cleanup(self) -> None:
        """Remove the temp file (e.g. on cancellation)."""
        if self._temp_path.exists():
            self._temp_path.unlink()
            logger.info("Cleaned up temp file: %s", self._temp_path)
