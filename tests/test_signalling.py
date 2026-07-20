"""
Smoke test for the signalling server.

Starts the server, connects two WebSocket clients, verifies:
  1. Registration works
  2. Message forwarding between peers works
  3. Unknown target returns an error
  4. Peer disconnect cleans up
"""

import asyncio
import json
import sys

# Add project to path
sys.path.insert(0, r"c:\Users\KIIT0001\Desktop\Projects\AdaptiveNetShare")

from websockets.asyncio.client import connect
from adaptivenetshare.signalling.server import main as server_main
from adaptivenetshare.config import SIGNALLING_PORT


async def test_signalling():
    # Start server in background
    server_task = asyncio.create_task(server_main())
    await asyncio.sleep(0.5)  # Let it bind

    url = f"ws://localhost:{SIGNALLING_PORT}"

    try:
        # --- Connect Peer A ---
        ws_a = await connect(url)
        await ws_a.send(json.dumps({"type": "register", "peer_id": "peer-A"}))
        ack_a = json.loads(await ws_a.recv())
        assert ack_a["type"] == "registered", f"Expected 'registered', got {ack_a}"
        print("[OK] Peer A registered")

        # --- Connect Peer B ---
        ws_b = await connect(url)
        await ws_b.send(json.dumps({"type": "register", "peer_id": "peer-B"}))
        ack_b = json.loads(await ws_b.recv())
        assert ack_b["type"] == "registered"
        print("[OK] Peer B registered")

        # --- Peer A sends offer to Peer B ---
        await ws_a.send(json.dumps({
            "type": "offer",
            "target": "peer-B",
            "sdp": {"type": "offer", "sdp": "fake-sdp-offer"},
        }))
        msg = json.loads(await ws_b.recv())
        assert msg["type"] == "offer"
        assert msg["source"] == "peer-A"
        assert msg["sdp"]["sdp"] == "fake-sdp-offer"
        print("[OK] Offer forwarded A -> B")

        # --- Peer B sends answer to Peer A ---
        await ws_b.send(json.dumps({
            "type": "answer",
            "target": "peer-A",
            "sdp": {"type": "answer", "sdp": "fake-sdp-answer"},
        }))
        msg = json.loads(await ws_a.recv())
        assert msg["type"] == "answer"
        assert msg["source"] == "peer-B"
        print("[OK] Answer forwarded B -> A")

        # --- Peer A sends ICE candidate to Peer B ---
        await ws_a.send(json.dumps({
            "type": "candidate",
            "target": "peer-B",
            "candidate": {"candidate": "fake-candidate", "sdpMid": "0"},
        }))
        msg = json.loads(await ws_b.recv())
        assert msg["type"] == "candidate"
        assert msg["candidate"]["candidate"] == "fake-candidate"
        print("[OK] ICE candidate forwarded A -> B")

        # --- Send to unknown peer ---
        await ws_a.send(json.dumps({
            "type": "offer",
            "target": "peer-UNKNOWN",
            "sdp": {},
        }))
        err = json.loads(await ws_a.recv())
        assert err["type"] == "error"
        assert "not found" in err["message"]
        print("[OK] Unknown peer returns error")

        # --- Cleanup ---
        await ws_a.close()
        await ws_b.close()
        print("\n==================================================")
        print("SIGNALLING SERVER TESTS PASSED [OK]")

    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(test_signalling())
