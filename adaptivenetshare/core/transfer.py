"""
Transfer orchestrator — coordinates the end-to-end file transfer flow.

SendTransfer  — sends a file to a connected peer.
ReceiveTransfer — receives a file from a connected peer.

Both classes operate on top of an already-open ``Peer`` data channel.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional, Callable

from adaptivenetshare.config import CHUNK_SIZE, DEFAULT_DOWNLOAD_DIR
from adaptivenetshare.core.chunker import FileSender, FileReceiver
from adaptivenetshare.core.integrity import verify_file
from adaptivenetshare.core.messages import (
    Manifest,
    ChunkMessage,
    AckMessage,
    NackMessage,
    DoneMessage,
    serialize,
    deserialize,
)
from adaptivenetshare.core.peer import Peer

logger = logging.getLogger(__name__)

# Optional progress callback: (chunks_done, total_chunks) → None
ProgressCallback = Optional[Callable[[int, int], None]]


class SendTransfer:
    """
    Drives the sender side of a file transfer.

    Flow:
        1. Send Manifest.
        2. Wait for Manifest ACK.
        3. Send chunks sequentially.
        4. For each chunk, wait for ACK or NACK (retransmit on NACK).
        5. Wait for Done message from receiver.
    """

    def __init__(
        self,
        peer: Peer,
        file_path: Path | str,
        chunk_size: int = CHUNK_SIZE,
        on_progress: ProgressCallback = None,
    ) -> None:
        self.peer = peer
        self.sender = FileSender(file_path, chunk_size)
        self.on_progress = on_progress
        self._ack_events: dict[int, asyncio.Event] = {}   # chunk_index → Event
        self._nack_indices: set[int] = set()
        self._manifest_acked = asyncio.Event()
        self._transfer_done = asyncio.Event()

    async def run(self) -> None:
        """Execute the full send transfer. Blocks until complete."""
        # Register our message handler on the peer
        self.peer.on_message(self._handle_incoming)

        # 1. Send manifest
        manifest = self.sender.get_manifest()
        logger.info(
            "Sending file: %s  (%d chunks)", manifest.filename, manifest.total_chunks
        )
        self.peer.send(serialize(manifest))

        # 2. Wait for manifest ACK
        await asyncio.wait_for(self._manifest_acked.wait(), timeout=30.0)
        logger.info("Manifest ACK received — starting chunk transfer")

        # 3. Send chunks sequentially
        acked_count = 0
        for idx in range(self.sender.total_chunks):
            await self._send_chunk_with_retry(idx, max_retries=3)
            acked_count += 1
            if self.on_progress:
                self.on_progress(acked_count, self.sender.total_chunks)

        # 4. Wait for Done
        await asyncio.wait_for(self._transfer_done.wait(), timeout=60.0)
        logger.info("Transfer complete: %s", manifest.filename)

    async def _send_chunk_with_retry(self, index: int, max_retries: int = 3) -> None:
        """Send a single chunk and wait for ACK. Retry on NACK."""
        for attempt in range(max_retries + 1):
            event = asyncio.Event()
            self._ack_events[index] = event
            self._nack_indices.discard(index)

            chunk = self.sender.get_chunk(index)
            self.peer.send(serialize(chunk))

            # Wait for ACK or NACK
            await asyncio.wait_for(event.wait(), timeout=30.0)

            if index not in self._nack_indices:
                # Got ACK
                return

            logger.warning(
                "Chunk %d NACKed (attempt %d/%d), retransmitting",
                index, attempt + 1, max_retries,
            )

        raise RuntimeError(f"Chunk {index} failed after {max_retries} retries")

    async def _handle_incoming(self, raw: bytes | str) -> None:
        """Process ACK / NACK / Done messages from the receiver."""
        if isinstance(raw, str):
            raw = raw.encode()

        msg = deserialize(raw)

        if isinstance(msg, AckMessage):
            if msg.chunk_index == -1:
                # Manifest ACK
                self._manifest_acked.set()
            else:
                event = self._ack_events.get(msg.chunk_index)
                if event:
                    event.set()

        elif isinstance(msg, NackMessage):
            self._nack_indices.add(msg.chunk_index)
            event = self._ack_events.get(msg.chunk_index)
            if event:
                event.set()

        elif isinstance(msg, DoneMessage):
            self._transfer_done.set()


class ReceiveTransfer:
    """
    Drives the receiver side of a file transfer.

    Flow:
        1. Wait for Manifest.
        2. Send Manifest ACK.
        3. For each incoming chunk: verify hash, write to disk, send ACK or NACK.
        4. When all chunks received, verify whole file, finalize, send Done.
    """

    def __init__(
        self,
        peer: Peer,
        download_dir: Path | str | None = None,
        on_progress: ProgressCallback = None,
    ) -> None:
        self.peer = peer
        self.download_dir = Path(download_dir) if download_dir else DEFAULT_DOWNLOAD_DIR
        self.on_progress = on_progress

        self._receiver: FileReceiver | None = None
        self._manifest_received = asyncio.Event()
        self._transfer_complete = asyncio.Event()
        self._result_path: Path | None = None

    async def run(self) -> Path:
        """Execute the full receive transfer. Blocks until complete. Returns final file path."""
        # Register our message handler on the peer
        self.peer.on_message(self._handle_incoming)

        # Wait for the transfer to complete
        await self._transfer_complete.wait()

        assert self._result_path is not None
        return self._result_path

    async def _handle_incoming(self, raw: bytes | str) -> None:
        """Process Manifest / Chunk messages from the sender."""
        if isinstance(raw, str):
            raw = raw.encode()

        msg = deserialize(raw)

        if isinstance(msg, Manifest):
            await self._handle_manifest(msg)

        elif isinstance(msg, ChunkMessage):
            await self._handle_chunk(msg)

    async def _handle_manifest(self, manifest: Manifest) -> None:
        """Set up the file receiver and ACK the manifest."""
        logger.info(
            "Received manifest: %s  (%d bytes, %d chunks)",
            manifest.filename, manifest.file_size, manifest.total_chunks,
        )
        self._receiver = FileReceiver(manifest, self.download_dir)
        self._manifest_received.set()

        # ACK the manifest
        ack = AckMessage(transfer_id=manifest.transfer_id, chunk_index=-1)
        self.peer.send(serialize(ack))

    async def _handle_chunk(self, chunk: ChunkMessage) -> None:
        """Verify a chunk, write to disk, and send ACK or NACK."""
        if self._receiver is None:
            logger.error("Received chunk before manifest — ignoring")
            return

        ok = self._receiver.receive_chunk(chunk)

        if ok:
            ack = AckMessage(
                transfer_id=chunk.transfer_id,
                chunk_index=chunk.chunk_index,
            )
            self.peer.send(serialize(ack))
        else:
            nack = NackMessage(
                transfer_id=chunk.transfer_id,
                chunk_index=chunk.chunk_index,
                reason="SHA-256 mismatch",
            )
            self.peer.send(serialize(nack))
            return

        if self.on_progress:
            self.on_progress(
                len(self._receiver.received_indices),
                self._receiver.manifest.total_chunks,
            )

        # Check if transfer is complete
        if self._receiver.is_complete():
            # Verify whole-file integrity
            final_path = self._receiver.finalize()

            if verify_file(final_path, self._receiver.manifest.sha256):
                logger.info("Whole-file hash verified ✓")
            else:
                logger.error("Whole-file hash MISMATCH — file may be corrupt")

            done = DoneMessage(transfer_id=self._receiver.manifest.transfer_id)
            self.peer.send(serialize(done))

            self._result_path = final_path
            self._transfer_complete.set()
