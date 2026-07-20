"""
WebSocket signalling server for AdaptiveNetShare.

Responsibilities (and *only* these):
  1. Register peers by ID.
  2. Forward SDP offers / answers between peers.
  3. Forward ICE candidates between peers.

No file data ever touches this server.  It is fully stateless beyond the
set of currently connected WebSocket clients.

Run locally:
    python -m adaptivenetshare.signalling.server

Deploy to Render.com by pointing the start command at this module.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict

from websockets.asyncio.server import serve, ServerConnection
from websockets.http11 import Response

from adaptivenetshare.config import SIGNALLING_HOST, SIGNALLING_PORT

logger = logging.getLogger(__name__)

# Maps peer_id → active WebSocket connection
_peers: Dict[str, ServerConnection] = {}


async def _handler(ws: ServerConnection) -> None:
    """Handle a single WebSocket client connection."""

    peer_id: str | None = None

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON",
                }))
                continue

            msg_type = msg.get("type")

            # ----------------------------------------------------------
            # REGISTER — client announces itself
            # ----------------------------------------------------------
            if msg_type == "register":
                peer_id = msg.get("peer_id")
                if not peer_id:
                    await ws.send(json.dumps({
                        "type": "error",
                        "message": "Missing peer_id in register message",
                    }))
                    continue

                _peers[peer_id] = ws
                logger.info("Registered peer %s  (total: %d)", peer_id, len(_peers))
                await ws.send(json.dumps({
                    "type": "registered",
                    "peer_id": peer_id,
                }))

            # ----------------------------------------------------------
            # OFFER / ANSWER / CANDIDATE / HANDSHAKE — forward to the target peer
            # ----------------------------------------------------------
            elif msg_type in ("offer", "answer", "candidate", "connection_request", "connection_accepted", "connection_rejected"):
                target_id = msg.get("target")
                if not target_id:
                    await ws.send(json.dumps({
                        "type": "error",
                        "message": f"Missing 'target' in {msg_type} message",
                    }))
                    continue

                target_ws = _peers.get(target_id)
                if target_ws is None:
                    await ws.send(json.dumps({
                        "type": "error",
                        "message": f"Peer {target_id!r} not found",
                    }))
                    continue

                # Inject the sender's identity so the receiver knows who
                # sent this message.
                msg["source"] = peer_id
                await target_ws.send(json.dumps(msg))
                logger.debug(
                    "Forwarded %s from %s → %s", msg_type, peer_id, target_id
                )

            else:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": f"Unknown message type: {msg_type!r}",
                }))

    except Exception:
        logger.exception("Connection error for peer %s", peer_id)

    finally:
        # Clean up on disconnect
        if peer_id and peer_id in _peers:
            del _peers[peer_id]
            logger.info("Unregistered peer %s  (total: %d)", peer_id, len(_peers))


def process_request(connection: ServerConnection, request) -> Response | None:
    """Intercept HTTP requests from Render health checks and return 200 OK."""
    if "Upgrade" not in request.headers:
        return Response(200, "OK", b"Healthy\n")
    return None

async def main() -> None:
    """Start the signalling server."""
    logger.info(
        "Signalling server starting on ws://%s:%d", SIGNALLING_HOST, SIGNALLING_PORT
    )
    async with serve(
        _handler, 
        SIGNALLING_HOST, 
        SIGNALLING_PORT,
        ping_interval=20,
        ping_timeout=20,
        process_request=process_request
    ) as server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    asyncio.run(main())
