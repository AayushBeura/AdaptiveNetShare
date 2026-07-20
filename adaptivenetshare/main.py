"""
AdaptiveNetShare — CLI entry point.

Usage:
    # Start the signalling server:
    python -m adaptivenetshare.main server

    # Send a file to a peer:
    python -m adaptivenetshare.main send --target <PEER_ID> --file <PATH>

    # Receive files (wait for incoming transfers):
    python -m adaptivenetshare.main receive [--download-dir <DIR>]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from adaptivenetshare.config import SIGNALLING_URL, DEFAULT_DOWNLOAD_DIR

logger = logging.getLogger("adaptivenetshare")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="adaptivenetshare",
        description="P2P file sharing over WebRTC",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--signalling-url",
        default=SIGNALLING_URL,
        help=f"WebSocket URL of the signalling server (default: {SIGNALLING_URL})",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ---- server ----
    sub.add_parser("server", help="Start the signalling server")

    # ---- send ----
    send_p = sub.add_parser("send", help="Send a file to a peer")
    send_p.add_argument("--target", required=True, help="Target peer ID")
    send_p.add_argument("--file", required=True, help="File path to send")
    send_p.add_argument("--peer-id", default=None, help="Override this peer's ID")

    # ---- receive ----
    recv_p = sub.add_parser("receive", help="Wait to receive files")
    recv_p.add_argument("--peer-id", default=None, help="Override this peer's ID")
    recv_p.add_argument(
        "--download-dir",
        default=str(DEFAULT_DOWNLOAD_DIR),
        help=f"Download directory (default: {DEFAULT_DOWNLOAD_DIR})",
    )

    # ---- gui ----
    sub.add_parser("gui", help="Start the desktop UI")

    return parser


# ------------------------------------------------------------------ #
# Command handlers
# ------------------------------------------------------------------ #

async def _run_server() -> None:
    from adaptivenetshare.signalling.server import main as server_main
    await server_main()


async def _run_send(args: argparse.Namespace) -> None:
    from pathlib import Path
    from adaptivenetshare.core.peer import Peer
    from adaptivenetshare.core.transfer import SendTransfer

    peer = Peer(peer_id=args.peer_id, signalling_url=args.signalling_url)
    try:
        await peer.connect_signalling()
        print(f"[sender]  Your peer ID: {peer.peer_id}")

        # Create offer to the target peer
        await peer.create_offer(args.target)
        print(f"[sender]  Offer sent to {args.target}, waiting for data channel...")

        # Wait for the data channel to open
        await asyncio.wait_for(peer.channel_ready.wait(), timeout=60.0)
        print("[sender]  Data channel open — starting transfer")

        def progress(done: int, total: int) -> None:
            pct = (done / total) * 100
            print(f"\r[sender]  Progress: {done}/{total} chunks ({pct:.1f}%)", end="", flush=True)

        transfer = SendTransfer(peer, Path(args.file), on_progress=progress)
        await transfer.run()
        print("\n[sender]  ✓ Transfer complete!")

    finally:
        await peer.close()


async def _run_receive(args: argparse.Namespace) -> None:
    from pathlib import Path
    from adaptivenetshare.core.peer import Peer
    from adaptivenetshare.core.transfer import ReceiveTransfer

    peer = Peer(peer_id=args.peer_id, signalling_url=args.signalling_url)
    try:
        await peer.connect_signalling()
        print(f"[receiver]  Your peer ID: {peer.peer_id}")
        print("[receiver]  Waiting for incoming connection...")

        # Wait for the data channel (set up by the offerer)
        await peer.channel_ready.wait()
        print("[receiver]  Data channel open — ready to receive")

        def progress(done: int, total: int) -> None:
            pct = (done / total) * 100
            print(f"\r[receiver]  Progress: {done}/{total} chunks ({pct:.1f}%)", end="", flush=True)

        transfer = ReceiveTransfer(
            peer,
            download_dir=Path(args.download_dir),
            on_progress=progress,
        )
        result = await transfer.run()
        print(f"\n[receiver]  ✓ File saved to: {result}")

    finally:
        await peer.close()


def main() -> None:
    # If no arguments provided, default to gui
    if len(sys.argv) == 1:
        sys.argv.append("gui")

    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if getattr(args, 'verbose', False) else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    if args.command == "server":
        asyncio.run(_run_server())
    elif args.command == "send":
        asyncio.run(_run_send(args))
    elif args.command == "receive":
        asyncio.run(_run_receive(args))
    elif args.command == "gui":
        from adaptivenetshare.ui.app import main as ui_main
        ui_main()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
