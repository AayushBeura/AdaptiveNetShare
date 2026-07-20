"""
AdaptiveNetShare — Premium Desktop Application.

A beautiful dark-themed desktop app for peer-to-peer file sharing
over WebRTC.  Built with customtkinter.

Threading model
───────────────
  Main thread   → customtkinter event loop (UI)
  Daemon thread → asyncio event loop (networking, WebRTC, transfers)

All UI updates from the async thread go through ``self.after(0, …)``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from tkinter import filedialog
from typing import Optional, Dict

import customtkinter as ctk

from adaptivenetshare.config import (
    SIGNALLING_URL,
    DEFAULT_DOWNLOAD_DIR,
    CHUNK_SIZE,
    SIGNALLING_HOST,
    SIGNALLING_PORT,
    SLIDING_WINDOW_SIZE,
)
from adaptivenetshare.core.peer import Peer
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

logger = logging.getLogger(__name__)

# ╔══════════════════════════════════════════════════════════════════╗
# ║  COLOUR PALETTE  (GitHub-Dark inspired)                        ║
# ╚══════════════════════════════════════════════════════════════════╝
BG            = "#0d1117"
BG_CARD       = "#161b22"
BG_INPUT      = "#0d1117"
BG_ELEVATED   = "#1c2333"
BORDER        = "#30363d"
TEXT          = "#e6edf3"
TEXT_DIM      = "#7d8590"
TEXT_MONO     = "#79c0ff"
ACCENT        = "#58a6ff"
ACCENT_HOVER  = "#79c0ff"
GREEN         = "#3fb950"
GREEN_DIM     = "#238636"
RED           = "#f85149"
ORANGE        = "#d29922"
PURPLE        = "#bc8cff"
CYAN          = "#39d2c0"

FONT          = "Segoe UI"
FONT_MONO     = "Consolas"


# ╔══════════════════════════════════════════════════════════════════╗
# ║  HELPERS                                                       ║
# ╚══════════════════════════════════════════════════════════════════╝

def _fmt_size(b: int | float) -> str:
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}" if unit != "B" else f"{int(b)} B"
        b /= 1024
    return f"{b:.1f} TB"


def _fmt_speed(bps: float) -> str:
    """Format bytes-per-second as human-readable string."""
    if bps < 1024:
        return f"{bps:.0f} B/s"
    elif bps < 1024 * 1024:
        return f"{bps / 1024:.1f} KB/s"
    else:
        return f"{bps / (1024 * 1024):.1f} MB/s"


def _fmt_eta(seconds: float) -> str:
    """Format seconds remaining as human-readable ETA."""
    if seconds < 0 or seconds > 86400:
        return "..."
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m {seconds % 60:.0f}s"
    return f"{seconds / 3600:.0f}h {(seconds % 3600) / 60:.0f}m"


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ASYNC BRIDGE — runs asyncio in a background thread            ║
# ╚══════════════════════════════════════════════════════════════════╝

class _AsyncBridge:
    """Runs an asyncio event loop on a daemon thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def submit(self, coro):
        """Schedule *coro* on the background loop.  Returns a Future."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def shutdown(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  MAIN APPLICATION                                              ║
# ╚══════════════════════════════════════════════════════════════════╝

def _get_or_create_peer_id() -> str:
    peer_id_file = Path.home() / ".adaptivenetshare_peer_id"
    if peer_id_file.exists():
        return peer_id_file.read_text().strip()
    new_id = str(uuid.uuid4())
    peer_id_file.write_text(new_id)
    return new_id

class App(ctk.CTk):
    """AdaptiveNetShare desktop application."""

    def __init__(self) -> None:
        super().__init__()

        # ── Window ────────────────────────────────────
        self.title("AdaptiveNetShare")
        self.geometry("920x740")
        self.minsize(820, 660)
        self.configure(fg_color=BG)
        ctk.set_appearance_mode("dark")

        # ── State ─────────────────────────────────────
        self.peer_id: str = _get_or_create_peer_id()
        self.peer: Optional[Peer] = None
        self.connected: bool = False
        self.selected_file: Optional[Path] = None
        self._signalling_url: str = SIGNALLING_URL
        self._download_dir: Path = DEFAULT_DOWNLOAD_DIR
        self._local_server = None

        # Send-side state
        self._send_manifest_ack: Optional[asyncio.Future[bool]] = None
        self._send_ack_queue: Optional[asyncio.Queue] = None
        self._send_done: Optional[asyncio.Event] = None
        self._current_send_future = None

        # Receive-side state
        self._receiver: Optional[FileReceiver] = None
        self._recv_transfer_id: Optional[str] = None
        self._recv_start_time: float = 0.0

        # Transfer widget references
        self._transfer_widgets: Dict[str, dict] = {}

        # ── Build UI ──────────────────────────────────
        self._build_ui()

        # ── Async bridge ──────────────────────────────
        self._bridge = _AsyncBridge()
        self._bridge.submit(self._init_async())

        self.protocol("WM_DELETE_WINDOW", self._on_closing)

    # ──────────────────────────────────────────────────
    #  UI CONSTRUCTION
    # ──────────────────────────────────────────────────

    def _build_ui(self) -> None:
        # Main scrollable container
        self._main = ctk.CTkScrollableFrame(
            self, fg_color="transparent",
            scrollbar_button_color=BORDER,
            scrollbar_button_hover_color=TEXT_DIM,
        )
        self._main.pack(fill="both", expand=True, padx=16, pady=(8, 0))

        self._build_header()
        self._build_identity_card()
        self._build_connection_card()
        self._build_transfer_card()
        self._build_transfers_list()
        self._build_status_bar()

    def _build_header(self) -> None:
        frame = ctk.CTkFrame(self._main, fg_color="transparent", height=60)
        frame.pack(fill="x", pady=(4, 14))

        # App icon (unicode)
        icon = ctk.CTkLabel(
            frame, text="\u26a1", font=(FONT, 28),
            text_color=ACCENT, width=40,
        )
        icon.pack(side="left", padx=(4, 8))

        title_box = ctk.CTkFrame(frame, fg_color="transparent")
        title_box.pack(side="left", fill="y")

        title = ctk.CTkLabel(
            title_box, text="AdaptiveNetShare",
            font=(FONT, 22, "bold"), text_color=TEXT, anchor="w",
        )
        title.pack(anchor="w")

        subtitle = ctk.CTkLabel(
            title_box, text="Secure peer-to-peer file sharing",
            font=(FONT, 11), text_color=TEXT_DIM, anchor="w",
        )
        subtitle.pack(anchor="w")

        # Connection indicator (right side)
        self._conn_frame = ctk.CTkFrame(frame, fg_color="transparent")
        self._conn_frame.pack(side="right", padx=8)

        self._conn_dot = ctk.CTkLabel(
            self._conn_frame, text="\u25cf", font=(FONT, 16),
            text_color=RED, width=20,
        )
        self._conn_dot.pack(side="left", padx=(0, 4))

        self._conn_label = ctk.CTkLabel(
            self._conn_frame, text="Offline",
            font=(FONT, 12), text_color=TEXT_DIM,
        )
        self._conn_label.pack(side="left")

    def _build_identity_card(self) -> None:
        card = ctk.CTkFrame(
            self._main, fg_color=BG_CARD, corner_radius=12,
            border_width=1, border_color=BORDER,
        )
        card.pack(fill="x", pady=(0, 10))

        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(fill="x", padx=16, pady=14)

        header = ctk.CTkLabel(
            inner, text="YOUR PEER ID",
            font=(FONT, 11, "bold"), text_color=TEXT_DIM, anchor="w",
        )
        header.pack(fill="x", pady=(0, 8))

        id_row = ctk.CTkFrame(inner, fg_color=BG_INPUT, corner_radius=8)
        id_row.pack(fill="x")

        self._id_label = ctk.CTkLabel(
            id_row, text=self.peer_id,
            font=(FONT_MONO, 13), text_color=PURPLE,
            anchor="w",
        )
        self._id_label.pack(side="left", fill="x", expand=True, padx=12, pady=10)

        self._copy_btn = ctk.CTkButton(
            id_row, text="Copy ID", width=80, height=32,
            font=(FONT, 12, "bold"), corner_radius=6,
            fg_color=ACCENT, hover_color=ACCENT_HOVER,
            text_color="#ffffff",
            command=self._on_copy_id,
        )
        self._copy_btn.pack(side="right", padx=8, pady=6)

        hint = ctk.CTkLabel(
            inner, text="Share this ID with your friend so they can connect to you",
            font=(FONT, 11), text_color=TEXT_DIM, anchor="w",
        )
        hint.pack(fill="x", pady=(6, 0))

    def _build_connection_card(self) -> None:
        card = ctk.CTkFrame(
            self._main, fg_color=BG_CARD, corner_radius=12,
            border_width=1, border_color=BORDER,
        )
        card.pack(fill="x", pady=(0, 10))

        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(fill="x", padx=16, pady=14)

        header = ctk.CTkLabel(
            inner, text="CONNECT TO PEER",
            font=(FONT, 11, "bold"), text_color=TEXT_DIM, anchor="w",
        )
        header.pack(fill="x", pady=(0, 8))

        row = ctk.CTkFrame(inner, fg_color="transparent")
        row.pack(fill="x")

        self._peer_entry = ctk.CTkEntry(
            row, placeholder_text="Paste your friend's Peer ID here...",
            font=(FONT_MONO, 13), height=40, corner_radius=8,
            fg_color=BG_INPUT, border_color=BORDER, border_width=1,
            text_color=TEXT, placeholder_text_color=TEXT_DIM,
        )
        self._peer_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))

        self._connect_btn = ctk.CTkButton(
            row, text="Connect", width=110, height=40,
            font=(FONT, 13, "bold"), corner_radius=8,
            fg_color=GREEN_DIM, hover_color=GREEN,
            text_color="#ffffff",
            command=self._on_connect_click,
        )
        self._connect_btn.pack(side="right")

        self._connect_status = ctk.CTkLabel(
            inner, text="Enter a Peer ID to establish a direct connection",
            font=(FONT, 11), text_color=TEXT_DIM, anchor="w",
        )
        self._connect_status.pack(fill="x", pady=(8, 0))

    def _build_transfer_card(self) -> None:
        card = ctk.CTkFrame(
            self._main, fg_color=BG_CARD, corner_radius=12,
            border_width=1, border_color=BORDER,
        )
        card.pack(fill="x", pady=(0, 10))

        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(fill="x", padx=16, pady=14)

        header = ctk.CTkLabel(
            inner, text="FILE TRANSFER",
            font=(FONT, 11, "bold"), text_color=TEXT_DIM, anchor="w",
        )
        header.pack(fill="x", pady=(0, 8))

        # File select row
        file_row = ctk.CTkFrame(inner, fg_color="transparent")
        file_row.pack(fill="x", pady=(0, 8))

        self._select_btn = ctk.CTkButton(
            file_row, text="\U0001f4c1  Select File", width=140, height=40,
            font=(FONT, 13), corner_radius=8,
            fg_color=BG_ELEVATED, hover_color=BORDER,
            border_width=1, border_color=BORDER,
            text_color=TEXT,
            command=self._on_select_file,
        )
        self._select_btn.pack(side="left", padx=(0, 10))

        self._file_label = ctk.CTkLabel(
            file_row, text="No file selected",
            font=(FONT, 12), text_color=TEXT_DIM, anchor="w",
        )
        self._file_label.pack(side="left", fill="x", expand=True)

        # Send button
        self._send_btn = ctk.CTkButton(
            inner, text="\U0001f680  Send File", height=44,
            font=(FONT, 14, "bold"), corner_radius=8,
            fg_color=ACCENT, hover_color=ACCENT_HOVER,
            text_color="#ffffff",
            state="disabled",
            command=self._on_send_click,
        )
        self._send_btn.pack(fill="x")

        self._send_hint = ctk.CTkLabel(
            inner,
            text="Connect to a peer first, then select a file to send",
            font=(FONT, 11), text_color=TEXT_DIM, anchor="w",
        )
        self._send_hint.pack(fill="x", pady=(6, 0))

    def _build_transfers_list(self) -> None:
        header_frame = ctk.CTkFrame(self._main, fg_color="transparent")
        header_frame.pack(fill="x", pady=(4, 6))

        ctk.CTkLabel(
            header_frame, text="TRANSFERS",
            font=(FONT, 11, "bold"), text_color=TEXT_DIM, anchor="w",
        ).pack(side="left")

        self._transfers_container = ctk.CTkFrame(
            self._main, fg_color=BG_CARD, corner_radius=12,
            border_width=1, border_color=BORDER,
        )
        self._transfers_container.pack(fill="both", expand=True, pady=(0, 10))

        self._no_transfers_label = ctk.CTkLabel(
            self._transfers_container,
            text="No transfers yet.\nSend or receive a file to get started.",
            font=(FONT, 12), text_color=TEXT_DIM,
            justify="center",
        )
        self._no_transfers_label.pack(expand=True, pady=30)

    def _build_status_bar(self) -> None:
        bar = ctk.CTkFrame(self, fg_color=BG_CARD, height=32, corner_radius=0)
        bar.pack(fill="x", side="bottom")

        self._status_label = ctk.CTkLabel(
            bar, text="Starting...",
            font=(FONT, 11), text_color=TEXT_DIM, anchor="w",
        )
        self._status_label.pack(side="left", padx=12, pady=4)

        self._signalling_label = ctk.CTkLabel(
            bar, text=f"Signalling: {self._signalling_url}",
            font=(FONT_MONO, 10), text_color=TEXT_DIM, anchor="e",
        )
        self._signalling_label.pack(side="right", padx=12, pady=4)

    # ──────────────────────────────────────────────────
    #  TRANSFER WIDGETS
    # ──────────────────────────────────────────────────

    def _add_transfer_widget(
        self, transfer_id: str, filename: str,
        direction: str, total_size: int,
    ) -> None:
        """Create a transfer progress card in the transfers list."""
        # Remove 'no transfers' placeholder
        self._no_transfers_label.pack_forget()

        frame = ctk.CTkFrame(
            self._transfers_container, fg_color=BG_ELEVATED,
            corner_radius=8,
        )
        frame.pack(fill="x", padx=10, pady=(8, 2))

        inner = ctk.CTkFrame(frame, fg_color="transparent")
        inner.pack(fill="x", padx=12, pady=10)

        # Header row: direction + filename + size
        top_row = ctk.CTkFrame(inner, fg_color="transparent")
        top_row.pack(fill="x", pady=(0, 6))

        arrow = "\u2191" if direction == "send" else "\u2193"
        arrow_color = CYAN if direction == "send" else GREEN
        verb = "Sending" if direction == "send" else "Receiving"

        ctk.CTkLabel(
            top_row, text=f"{arrow} {verb}",
            font=(FONT, 12, "bold"), text_color=arrow_color,
        ).pack(side="left", padx=(0, 8))

        ctk.CTkLabel(
            top_row, text=filename,
            font=(FONT, 12), text_color=TEXT,
        ).pack(side="left")

        ctk.CTkLabel(
            top_row, text=_fmt_size(total_size),
            font=(FONT, 11), text_color=TEXT_DIM,
        ).pack(side="right")

        # Progress bar
        progress = ctk.CTkProgressBar(
            inner, height=8, corner_radius=4,
            fg_color=BORDER, progress_color=arrow_color,
        )
        progress.pack(fill="x", pady=(0, 6))
        progress.set(0)

        # Stats row
        stats = ctk.CTkLabel(
            inner, text="Preparing...",
            font=(FONT, 11), text_color=TEXT_DIM, anchor="w",
        )
        stats.pack(fill="x")

        self._transfer_widgets[transfer_id] = {
            "frame": frame,
            "progress": progress,
            "stats": stats,
            "direction": direction,
            "color": arrow_color,
        }

    def _update_transfer_widget(
        self, transfer_id: str, progress: float,
        speed: float, eta: float,
        chunks_done: int, total_chunks: int,
    ) -> None:
        """Update an existing transfer card's progress."""
        w = self._transfer_widgets.get(transfer_id)
        if not w:
            return
        w["progress"].set(progress)
        pct = progress * 100
        stats_text = (
            f"{pct:.1f}%  |  {_fmt_speed(speed)}  |  "
            f"ETA: {_fmt_eta(eta)}  |  {chunks_done}/{total_chunks} chunks"
        )
        w["stats"].configure(text=stats_text)

    def _complete_transfer_widget(self, transfer_id: str, extra: str = "") -> None:
        """Mark a transfer as completed."""
        w = self._transfer_widgets.get(transfer_id)
        if not w:
            return
        w["progress"].set(1.0)
        w["progress"].configure(progress_color=GREEN)
        msg = "Completed"
        if extra:
            msg += f" - {extra}"
        w["stats"].configure(text=msg, text_color=GREEN)

    def _fail_transfer_widget(self, transfer_id: str, reason: str) -> None:
        """Mark a transfer as failed."""
        w = self._transfer_widgets.get(transfer_id)
        if not w:
            return
        w["progress"].configure(progress_color=RED)
        w["stats"].configure(text=f"Failed: {reason}", text_color=RED)

    # ──────────────────────────────────────────────────
    #  UI CALLBACKS (main thread)
    # ──────────────────────────────────────────────────

    def _on_copy_id(self) -> None:
        self.clipboard_clear()
        self.clipboard_append(self.peer_id)
        self._copy_btn.configure(text="Copied!", fg_color=GREEN)
        self.after(2000, lambda: self._copy_btn.configure(
            text="Copy ID", fg_color=ACCENT
        ))

    def _on_connect_click(self) -> None:
        if self.connected:
            self._connect_btn.configure(text="Disconnecting...", state="disabled")
            self._bridge.submit(self._disconnect_async())
            return

        target_id = self._peer_entry.get().strip()
        if not target_id:
            self._connect_status.configure(
                text="Please enter a Peer ID", text_color=ORANGE,
            )
            return
        if target_id == self.peer_id:
            self._connect_status.configure(
                text="Cannot connect to yourself", text_color=RED,
            )
            return
        self._connect_btn.configure(text="Connecting...", state="disabled")
        self._connect_status.configure(
            text="Establishing WebRTC connection...", text_color=ORANGE,
        )
        self._bridge.submit(self._connect_to_peer_async(target_id))

    def _on_select_file(self) -> None:
        path = filedialog.askopenfilename(title="Select File to Send")
        if not path:
            return
        self.selected_file = Path(path)
        size = self.selected_file.stat().st_size
        self._file_label.configure(
            text=f"{self.selected_file.name}  ({_fmt_size(size)})",
            text_color=TEXT,
        )
        if self.connected:
            self._send_btn.configure(state="normal")
            self._send_hint.configure(text="Ready to send!", text_color=GREEN)

    def _on_send_click(self) -> None:
        if not self.connected or self.selected_file is None:
            return
        self._send_btn.configure(state="disabled", text="Sending...")
        self._send_hint.configure(text="Transfer in progress...", text_color=CYAN)
        self._current_send_future = self._bridge.submit(self._send_file_async(self.selected_file))

    def _on_peer_connected(self) -> None:
        """Called (on main thread) when the data channel opens."""
        self.connected = True
        self._conn_dot.configure(text_color=GREEN)
        self._conn_label.configure(text="Connected", text_color=GREEN)
        self._connect_btn.configure(
            text="Disconnect", fg_color=RED, state="normal",
        )
        self._connect_status.configure(
            text="Direct P2P channel established", text_color=GREEN,
        )
        if self.selected_file:
            self._send_btn.configure(state="normal")
            self._send_hint.configure(text="Ready to send!", text_color=GREEN)
        else:
            self._send_hint.configure(
                text="Select a file to send to your peer", text_color=TEXT_DIM,
            )
        self._status_label.configure(text="Connected - ready to transfer files")

    def _on_peer_disconnected(self) -> None:
        """Called (on main thread) when the peer connection is closed or failed."""
        self.connected = False
        self._conn_dot.configure(text_color=ORANGE)
        self._conn_label.configure(text="Online", text_color=ORANGE)
        self._connect_btn.configure(
            text="Connect", fg_color=GREEN_DIM, state="normal",
        )
        self._connect_status.configure(
            text="Connection closed", text_color=TEXT_DIM,
        )
        self._send_btn.configure(state="disabled")
        self._send_hint.configure(
            text="Connect to a peer first, then select a file to send",
            text_color=TEXT_DIM,
        )
        self._status_label.configure(text="Disconnected from peer")
        
        # Cancel any active sends
        if self._current_send_future:
            self._current_send_future.cancel()
            self._current_send_future = None
            
        # Fail any active receives
        if self._recv_transfer_id:
            self._fail_transfer_widget(self._recv_transfer_id, "Connection closed")
            self._recv_transfer_id = None
            self._receiver = None

    async def _disconnect_async(self) -> None:
        """Perform background disconnection."""
        if self.peer:
            await self.peer.disconnect_peer()

    def _set_status(self, text: str, color: str = TEXT_DIM) -> None:
        self._status_label.configure(text=text, text_color=color)

    # ──────────────────────────────────────────────────
    #  ASYNC OPERATIONS (background thread)
    # ──────────────────────────────────────────────────

    async def _init_async(self) -> None:
        """Auto-start local signalling server (if localhost) and register."""
        # Try to start local server when using localhost
        if "localhost" in self._signalling_url or "127.0.0.1" in self._signalling_url:
            try:
                from websockets.asyncio.server import serve
                from adaptivenetshare.signalling.server import _handler

                self._local_server = await serve(
                    _handler, SIGNALLING_HOST, SIGNALLING_PORT,
                )
                self.after(0, self._set_status,
                           f"Local signalling server started on port {SIGNALLING_PORT}")
                logger.info("Auto-started local signalling server on port %d",
                            SIGNALLING_PORT)
            except OSError:
                self.after(0, self._set_status,
                           "Connected to existing signalling server")
                logger.info("Signalling server already running on port %d",
                            SIGNALLING_PORT)

        # Connect peer to signalling server
        try:
            self.peer = Peer(
                peer_id=self.peer_id,
                signalling_url=self._signalling_url,
            )
            await self.peer.connect_signalling()

            # Register callbacks
            self.peer.on_message(self._handle_message)
            self.peer.on_connection_request(self._handle_connection_request)
            self.peer.on_data_channel_ready(
                lambda: self.after(0, self._on_peer_connected)
            )
            self.peer.on_connection_closed(
                lambda: self.after(0, self._on_peer_disconnected)
            )

            self.after(0, self._conn_dot.configure, {"text_color": ORANGE})
            self.after(0, self._conn_label.configure,
                       {"text": "Online", "text_color": ORANGE})
            self.after(0, self._set_status,
                       "Registered with signalling server - waiting for connections")
            logger.info("Peer %s registered with signalling server", self.peer_id)

        except Exception as e:
            logger.exception("Failed to connect to signalling server")
            self.after(0, self._set_status,
                       f"Signalling server error: {e}", RED)
            self.after(0, self._conn_label.configure,
                       {"text": "Offline", "text_color": RED})

    def _handle_connection_request(self, target_id: str) -> None:
        """Handle incoming connection request from a peer."""
        def ask_user():
            from tkinter import messagebox
            res = messagebox.askyesno(
                "Incoming Connection",
                f"Peer {target_id} wants to connect with you.\n\nAccept connection?"
            )
            if res:
                if self.peer:
                    self._bridge.submit(self.peer.accept_connection(target_id))
                    # Auto-fill the peer ID so receiver can easily send back
                    self._peer_entry.delete(0, "end")
                    self._peer_entry.insert(0, target_id)
                    self._connect_status.configure(
                        text="Establishing WebRTC connection...", text_color=ORANGE
                    )
            else:
                if self.peer:
                    self._bridge.submit(self.peer.reject_connection(target_id))

        self.after(0, ask_user)

    async def _connect_to_peer_async(self, target_id: str) -> None:
        """Create a WebRTC offer to the target peer."""
        try:
            if self.peer is None:
                raise RuntimeError("Not connected to signalling server")

            self.after(0, self._connect_status.configure,
                       {"text": "Requesting connection...", "text_color": ORANGE})
            
            accepted = await self.peer.request_connection(target_id)
            if not accepted:
                raise RuntimeError("Connection request declined by peer")

            self.after(0, self._connect_status.configure,
                       {"text": "Establishing WebRTC connection...", "text_color": ORANGE})

            await self.peer.create_offer(target_id)
            logger.info("Sent offer to %s", target_id)

            # Wait for the data channel to open (timeout 60s)
            await asyncio.wait_for(self.peer.channel_ready.wait(), timeout=60)
            # _on_peer_connected is called by the channel_ready callback

        except asyncio.TimeoutError:
            self.after(0, self._connect_status.configure,
                       {"text": "Connection timed out. Check the Peer ID.",
                        "text_color": RED})
            self.after(0, self._connect_btn.configure,
                       {"text": "Connect", "state": "normal",
                        "fg_color": GREEN_DIM})

        except Exception as e:
            logger.exception("Connection failed")
            self.after(0, self._connect_status.configure,
                       {"text": f"Failed: {e}", "text_color": RED})
            self.after(0, self._connect_btn.configure,
                       {"text": "Connect", "state": "normal",
                        "fg_color": GREEN_DIM})

    async def _send_file_async(self, file_path: Path) -> None:
        """Full send flow: manifest → chunks → done."""
        transfer_id = ""
        try:
            sender = FileSender(file_path, CHUNK_SIZE)
            manifest = sender.get_manifest()
            transfer_id = manifest.transfer_id

            # Initialise send-side events
            self._send_manifest_ack = self._bridge.loop.create_future()
            self._send_ack_queue = asyncio.Queue()
            self._send_done = asyncio.Event()

            # Add transfer widget
            self.after(0, self._add_transfer_widget,
                       transfer_id, manifest.filename,
                       "send", manifest.file_size)

            # 1. Send manifest
            self.peer.send(serialize(manifest))

            # 2. Wait for manifest ACK/NACK
            approved = await asyncio.wait_for(self._send_manifest_ack, timeout=30)
            if not approved:
                raise RuntimeError("Receiver declined the transfer")

            # 3. Send chunks with Sliding Window
            start_time = time.time()
            bytes_sent = 0

            window_size = SLIDING_WINDOW_SIZE
            in_flight = set()
            next_to_send = 0

            while next_to_send < sender.total_chunks or in_flight:
                # 3a. Fill the window
                while len(in_flight) < window_size and next_to_send < sender.total_chunks:
                    chunk = sender.get_chunk(next_to_send)
                    in_flight.add(next_to_send)
                    await self.peer.send_with_backpressure(serialize(chunk))
                    next_to_send += 1

                # 3b. Wait for an ACK/NACK
                try:
                    status, idx = await asyncio.wait_for(
                        self._send_ack_queue.get(), timeout=30.0
                    )

                    if status == "ACK":
                        if idx in in_flight:
                            in_flight.remove(idx)
                            bytes_sent += sender.chunk_size
                            
                            elapsed = time.time() - start_time
                            speed = bytes_sent / elapsed if elapsed > 0 else 0
                            remaining = max(0, manifest.file_size - bytes_sent)
                            eta = remaining / speed if speed > 0 else 0
                            progress = min(1.0, bytes_sent / manifest.file_size)

                            self.after(0, self._update_transfer_widget,
                                       transfer_id, progress, speed, eta,
                                       min(int(bytes_sent / sender.chunk_size), sender.total_chunks), 
                                       sender.total_chunks)
                                       
                    elif status == "NACK":
                        logger.warning("Chunk %d NACKed, retransmitting", idx)
                        # Retransmit just this chunk
                        chunk = sender.get_chunk(idx)
                        await self.peer.send_with_backpressure(serialize(chunk))

                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for ACKs. Retransmitting in-flight window.")
                    for idx in in_flight:
                        chunk = sender.get_chunk(idx)
                        await self.peer.send_with_backpressure(serialize(chunk))

            # 4. Wait for Done
            await asyncio.wait_for(self._send_done.wait(), timeout=60)

            self.after(0, self._complete_transfer_widget,
                       transfer_id, f"Sent {_fmt_size(manifest.file_size)}")
            self.after(0, self._send_hint.configure,
                       {"text": "Transfer complete!", "text_color": GREEN})

        except asyncio.CancelledError:
            logger.warning("Send cancelled due to disconnect")
            if transfer_id:
                self.after(0, self._fail_transfer_widget, transfer_id, "Connection closed")
            self.after(0, self._send_hint.configure,
                       {"text": "Send failed: Connection closed", "text_color": RED})

        except Exception as e:
            logger.exception("Send failed")
            if transfer_id:
                self.after(0, self._fail_transfer_widget, transfer_id, str(e))
            self.after(0, self._send_hint.configure,
                       {"text": f"Send failed: {e}", "text_color": RED})

        finally:
            self.after(0, self._send_btn.configure,
                       {"state": "normal" if self.connected else "disabled",
                        "text": "\U0001f680  Send File"})
            self.selected_file = None
            self.after(0, self._file_label.configure,
                       {"text": "No file selected", "text_color": TEXT_DIM})

    # ──────────────────────────────────────────────────
    #  MESSAGE ROUTER (background thread)
    # ──────────────────────────────────────────────────

    async def _handle_message(self, raw: bytes | str) -> None:
        """
        Central message dispatcher.

        Routes incoming data-channel messages to the correct handler
        based on message type:
          - Manifest, ChunkMessage → receiver
          - AckMessage, NackMessage, DoneMessage → sender
        """
        if isinstance(raw, str):
            raw = raw.encode()

        try:
            msg = deserialize(raw)
        except Exception:
            logger.exception("Failed to deserialize message")
            return

        # ── Receive-side messages ────────────────────
        if isinstance(msg, Manifest):
            await self._handle_manifest(msg)

        elif isinstance(msg, ChunkMessage):
            await self._handle_chunk(msg)

        # ── Send-side messages ───────────────────────
        elif isinstance(msg, AckMessage):
            if msg.chunk_index == -1:
                # Manifest ACK
                if self._send_manifest_ack and not self._send_manifest_ack.done():
                    self._send_manifest_ack.set_result(True)
            else:
                if self._send_ack_queue:
                    self._send_ack_queue.put_nowait(("ACK", msg.chunk_index))

        elif isinstance(msg, NackMessage):
            if msg.chunk_index == -1:
                # Manifest NACK
                if self._send_manifest_ack and not self._send_manifest_ack.done():
                    self._send_manifest_ack.set_result(False)
            else:
                if self._send_ack_queue:
                    self._send_ack_queue.put_nowait(("NACK", msg.chunk_index))

        elif isinstance(msg, DoneMessage):
            if self._send_done:
                self._send_done.set()

    async def _handle_manifest(self, manifest: Manifest) -> None:
        """Start receiving a file, asking user for confirmation first."""
        logger.info("Incoming file: %s (%d bytes)",
                    manifest.filename, manifest.file_size)

        # Prompt user on main thread
        approved_event = asyncio.Event()
        approved = False

        def ask_user():
            nonlocal approved
            from tkinter import messagebox
            res = messagebox.askyesno(
                "Incoming File Transfer",
                f"Your peer wants to send you a file:\n\n"
                f"Filename: {manifest.filename}\n"
                f"Size: {_fmt_size(manifest.file_size)}\n\n"
                f"Do you want to accept this transfer?"
            )
            approved = res
            self._bridge.loop.call_soon_threadsafe(approved_event.set)

        self.after(0, ask_user)
        await approved_event.wait()

        if not approved:
            logger.info("User declined the file transfer")
            nack = NackMessage(
                transfer_id=manifest.transfer_id,
                chunk_index=-1,
                reason="Decline",
            )
            self.peer.send(serialize(nack))
            return

        self._receiver = FileReceiver(manifest, self._download_dir)
        self._recv_transfer_id = manifest.transfer_id
        self._recv_start_time = time.time()

        # Show in UI
        self.after(0, self._add_transfer_widget,
                   manifest.transfer_id, manifest.filename,
                   "receive", manifest.file_size)

        # ACK the manifest
        ack = AckMessage(transfer_id=manifest.transfer_id, chunk_index=-1)
        self.peer.send(serialize(ack))

    async def _handle_chunk(self, chunk: ChunkMessage) -> None:
        """Process an incoming chunk."""
        if self._receiver is None:
            logger.warning("Received chunk but no active receiver")
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

        # Update progress
        done_count = len(self._receiver.received_indices)
        total = self._receiver.manifest.total_chunks
        elapsed = time.time() - self._recv_start_time
        bytes_done = done_count * self._receiver.manifest.chunk_size
        speed = bytes_done / elapsed if elapsed > 0 else 0
        remaining = self._receiver.manifest.file_size - bytes_done
        eta = remaining / speed if speed > 0 else 0
        progress = done_count / total

        self.after(0, self._update_transfer_widget,
                   chunk.transfer_id, progress, speed, eta,
                   done_count, total)

        # Check completion
        if self._receiver.is_complete():
            final_path = self._receiver.finalize()

            if verify_file(final_path, self._receiver.manifest.sha256):
                logger.info("Whole-file hash verified OK")
                self.after(0, self._complete_transfer_widget,
                           chunk.transfer_id,
                           f"Saved to {final_path.name}")
            else:
                logger.error("Whole-file hash MISMATCH")
                self.after(0, self._fail_transfer_widget,
                           chunk.transfer_id,
                           "File hash mismatch - may be corrupt")

            # Send Done to sender
            done = DoneMessage(transfer_id=self._receiver.manifest.transfer_id)
            self.peer.send(serialize(done))

            self._receiver = None
            self._recv_transfer_id = None

    # ──────────────────────────────────────────────────
    #  CLEANUP
    # ──────────────────────────────────────────────────

    def _on_closing(self) -> None:
        """Gracefully shut down."""
        async def _cleanup():
            if self.peer:
                try:
                    await self.peer.close()
                except Exception:
                    pass
            if self._local_server:
                self._local_server.close()
                try:
                    await self._local_server.wait_closed()
                except Exception:
                    pass

        try:
            future = self._bridge.submit(_cleanup())
            future.result(timeout=3)
        except Exception:
            pass

        self._bridge.shutdown()
        self.destroy()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ENTRY POINT                                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    ctk.set_appearance_mode("dark")
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
