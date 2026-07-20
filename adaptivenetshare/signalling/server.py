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

        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                msg_type = data.get("type")
                
                if msg_type == "register":
                    peer_id = data.get("peer_id")
                    if peer_id:
                        _peers[peer_id] = ws
                        logger.info("Registered peer %s  (total: %d)", peer_id, len(_peers))
                        await ws.send_json({"type": "registered"})
                
                elif msg_type in ("offer", "answer", "candidate", "connection_request", "connection_accepted", "connection_rejected"):
                    target = data.get("target")
                    if target in _peers:
                        data["source"] = peer_id
                        await _peers[target].send_json(data)
                    else:
                        await ws.send_json({"type": "error", "message": f"Peer {target} not found"})
                        
            elif msg.type == web.WSMsgType.ERROR:
                logger.error('ws connection closed with exception %s', ws.exception())
    finally:
        if peer_id and peer_id in _peers:
            del _peers[peer_id]
            logger.info("Unregistered peer %s  (total: %d)", peer_id, len(_peers))
            
    return ws

async def index_handler(request: web.Request) -> web.StreamResponse:
    """Handle both HTTP health checks and WebSocket upgrades."""
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await websocket_handler(request)
    return web.Response(text="Healthy\n", status=200)

async def main() -> None:
    """Start the signalling server."""
    logger.info(
        "Signalling server starting on http://%s:%d", SIGNALLING_HOST, SIGNALLING_PORT
    )
    app = web.Application()
    app.router.add_route('*', '/', index_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, SIGNALLING_HOST, SIGNALLING_PORT)
    await site.start()
    
    # Run forever
    await asyncio.Event().wait()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    asyncio.run(main())
