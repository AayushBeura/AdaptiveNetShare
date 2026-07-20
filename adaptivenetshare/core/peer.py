"""
WebRTC peer connection manager.

Handles the full lifecycle of a peer-to-peer connection:
  1. Connect to the signalling server via WebSocket.
  2. Exchange SDP offers / answers and ICE candidates.
  3. Open a WebRTC data channel for file transfer.

Uses aiortc for the WebRTC implementation and the signalling server
from ``adaptivenetshare.signalling.server`` for rendezvous.
"""

from __future__ import annotations

import asyncio
import aiohttp
import json
import logging
import uuid
from typing import Callable, Optional, Awaitable

from aiortc import (
    RTCPeerConnection,
    RTCConfiguration,
    RTCIceServer,
    RTCSessionDescription,
    RTCIceCandidate,
)
from websockets.asyncio.client import connect as ws_connect

from adaptivenetshare.config import (
    SIGNALLING_URL,
    STUN_URLS,
    METERED_API_URL,
    DATA_CHANNEL_LABEL,
)

logger = logging.getLogger(__name__)

# Type alias for message callbacks
MessageCallback = Callable[[bytes | str], Awaitable[None] | None]
ChannelReadyCallback = Callable[[], Awaitable[None] | None]


async def _build_ice_config() -> RTCConfiguration:
    """Build the RTCConfiguration by fetching dynamic credentials from Metered."""
    ice_servers = [RTCIceServer(urls=STUN_URLS)]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(METERED_API_URL, timeout=5.0) as response:
                if response.status == 200:
                    data = await response.json()
                    for s in data:
                        urls = s.get("urls")
                        if isinstance(urls, str):
                            urls = [urls]
                        ice_servers.append(
                            RTCIceServer(
                                urls=urls,
                                username=s.get("username"),
                                credential=s.get("credential")
                            )
                        )
                else:
                    logger.error("Failed to fetch ICE servers, HTTP status %s", response.status)
    except Exception as e:
        logger.error("Error fetching ICE servers from REST API: %s", e)
        
    return RTCConfiguration(iceServers=ice_servers)


class Peer:
    """
    Manages a single WebRTC peer connection with signalling.

    Usage (offerer)::

        peer = Peer()
        await peer.connect_signalling()
        peer.on_data_channel_ready(my_ready_handler)
        peer.on_message(my_message_handler)
        await peer.create_offer(target_peer_id)
        # ... data channel opens, callbacks fire ...
        await peer.close()

    Usage (answerer)::

        peer = Peer()
        await peer.connect_signalling()
        peer.on_data_channel_ready(my_ready_handler)
        peer.on_message(my_message_handler)
        # ... answerer waits; signalling loop handles incoming offer ...
        await peer.close()
    """

    def __init__(
        self,
        peer_id: str | None = None,
        signalling_url: str = SIGNALLING_URL,
    ) -> None:
        self.peer_id = peer_id or str(uuid.uuid4())
        self.signalling_url = signalling_url

        self._ws = None                         # WebSocket to signalling server
        self._pc: RTCPeerConnection | None = None
        self._channel = None                    # RTCDataChannel (offerer creates)
        self._signalling_task: asyncio.Task | None = None

        # User-registered callbacks
        self._on_message: MessageCallback | None = None
        self._on_channel_ready: ChannelReadyCallback | None = None
        self._on_connection_closed: Callable[[], None] | None = None
        self._on_connection_request: Callable[[str], None] | None = None

        # Events and Futures
        self.channel_ready = asyncio.Event()
        self.buffer_low = asyncio.Event()
        self.buffer_low.set()  # Initially low
        self._connection_request_futures: dict[str, asyncio.Future] = {}

    # ------------------------------------------------------------------ #
    # Callback registration
    # ------------------------------------------------------------------ #

    def on_message(self, callback: MessageCallback) -> None:
        """Register a callback for incoming data-channel messages."""
        self._on_message = callback

    def on_data_channel_ready(self, callback: ChannelReadyCallback) -> None:
        """Register a callback for when the data channel opens."""
        self._on_channel_ready = callback

    def on_connection_closed(self, callback: Callable[[], None]) -> None:
        """Register a callback for when the connection closes."""
        self._on_connection_closed = callback

    def on_connection_request(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming connection requests."""
        self._on_connection_request = callback

    # ------------------------------------------------------------------ #
    # Signalling
    # ------------------------------------------------------------------ #

    async def connect_signalling(self) -> None:
        """Connect to the WebSocket signalling server and register."""
        for attempt in range(3):
            try:
                # Render free tier might take ~50s to wake up, so use a longer timeout or retry
                self._ws = await ws_connect(
                    self.signalling_url, open_timeout=60.0,
                    ping_interval=20, ping_timeout=20
                )
                break
            except Exception as e:
                if attempt == 2:
                    logger.error("Failed to connect to signalling server after 3 attempts.")
                    raise
                logger.warning(f"Signalling connection attempt {attempt+1} failed: {e}. Retrying in 2s...")
                await asyncio.sleep(2)

        # Register
        await self._ws.send(json.dumps({
            "type": "register",
            "peer_id": self.peer_id,
        }))

        # Read registration acknowledgement
        ack = json.loads(await self._ws.recv())
        if ack.get("type") != "registered":
            raise RuntimeError(f"Registration failed: {ack}")

        logger.info("Registered with signalling server as %s", self.peer_id)

        # Start background task to process incoming signalling messages
        self._signalling_task = asyncio.create_task(self._signalling_loop())

    async def _signalling_loop(self) -> None:
        """Process incoming signalling messages (offers, answers, candidates)."""
        assert self._ws is not None
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "offer":
                    await self._handle_offer(msg)
                elif msg_type == "answer":
                    await self._handle_answer(msg)
                elif msg_type == "candidate":
                    await self._handle_candidate(msg)
                elif msg_type == "connection_request":
                    source_id = msg.get("source")
                    if source_id and self._on_connection_request:
                        self._on_connection_request(source_id)
                elif msg_type == "connection_accepted":
                    source_id = msg.get("source")
                    if source_id in self._connection_request_futures:
                        self._connection_request_futures[source_id].set_result(True)
                elif msg_type == "connection_rejected":
                    source_id = msg.get("source")
                    if source_id in self._connection_request_futures:
                        self._connection_request_futures[source_id].set_result(False)
                elif msg_type == "error":
                    error_msg = msg.get("message", "")
                    logger.error("Signalling error: %s", error_msg)
                    if "not found" in error_msg:
                        # Fail pending requests
                        for f in self._connection_request_futures.values():
                            if not f.done():
                                f.set_exception(RuntimeError(error_msg))
                else:
                    logger.warning("Unknown signalling message: %s", msg_type)
        except Exception:
            logger.exception("Signalling loop error")

    # ------------------------------------------------------------------ #
    # Offer / Answer
    # ------------------------------------------------------------------ #

    async def request_connection(self, target_id: str) -> bool:
        """Request a connection with the target peer. Returns True if accepted."""
        if target_id == self.peer_id:
            raise ValueError("Cannot connect to yourself")

        assert self._ws is not None
        fut = asyncio.get_running_loop().create_future()
        self._connection_request_futures[target_id] = fut

        await self._ws.send(json.dumps({
            "type": "connection_request",
            "target": target_id,
        }))
        logger.info("Sent connection request to %s", target_id)

        try:
            return await asyncio.wait_for(fut, timeout=30.0)
        finally:
            self._connection_request_futures.pop(target_id, None)

    async def accept_connection(self, target_id: str) -> None:
        """Accept an incoming connection request."""
        if self._ws is not None:
            await self._ws.send(json.dumps({
                "type": "connection_accepted",
                "target": target_id,
            }))

    async def reject_connection(self, target_id: str) -> None:
        """Reject an incoming connection request."""
        if self._ws is not None:
            await self._ws.send(json.dumps({
                "type": "connection_rejected",
                "target": target_id,
            }))

    async def create_offer(self, target_id: str) -> None:
        """
        Create a WebRTC offer to connect to *target_id*.

        This side becomes the offerer and creates the data channel.
        """
        if target_id == self.peer_id:
            raise ValueError("Cannot connect to yourself")

        self._pc = RTCPeerConnection(configuration=await _build_ice_config())
        self._setup_pc_events()

        # The offerer creates the data channel
        self._channel = self._pc.createDataChannel(DATA_CHANNEL_LABEL)
        self._setup_channel_events(self._channel)

        # Create and set local SDP offer
        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)

        # Wait for ICE gathering to complete to embed candidates in SDP
        await self._wait_for_ice_gathering()

        # Send offer through signalling server
        assert self._ws is not None
        await self._ws.send(json.dumps({
            "type": "offer",
            "target": target_id,
            "sdp": {
                "type": self._pc.localDescription.type,
                "sdp": self._pc.localDescription.sdp,
            },
        }))
        logger.info("Sent SDP offer to %s", target_id)

    async def _handle_offer(self, msg: dict) -> None:
        """Handle an incoming SDP offer from another peer."""
        source_id = msg.get("source", "unknown")
        logger.info("Received SDP offer from %s", source_id)

        self._pc = RTCPeerConnection(configuration=await _build_ice_config())
        self._setup_pc_events()

        # Set remote description from the offer
        sdp_data = msg["sdp"]
        offer = RTCSessionDescription(sdp=sdp_data["sdp"], type=sdp_data["type"])
        await self._pc.setRemoteDescription(offer)

        # Create and set the answer
        answer = await self._pc.createAnswer()
        await self._pc.setLocalDescription(answer)

        # Wait for ICE gathering to complete to embed candidates in SDP
        await self._wait_for_ice_gathering()

        # Send answer back through signalling server
        assert self._ws is not None
        await self._ws.send(json.dumps({
            "type": "answer",
            "target": source_id,
            "sdp": {
                "type": self._pc.localDescription.type,
                "sdp": self._pc.localDescription.sdp,
            },
        }))
        logger.info("Sent SDP answer to %s", source_id)

    async def _handle_answer(self, msg: dict) -> None:
        """Handle an incoming SDP answer."""
        if self._pc is None:
            logger.warning("Received answer but no PeerConnection exists")
            return

        sdp_data = msg["sdp"]
        answer = RTCSessionDescription(sdp=sdp_data["sdp"], type=sdp_data["type"])
        await self._pc.setRemoteDescription(answer)
        logger.info("Set remote description from answer")

    async def _handle_candidate(self, msg: dict) -> None:
        """Handle an incoming ICE candidate (unused in full-SDP exchange but kept for compatibility)."""
        if self._pc is None:
            logger.warning("Received ICE candidate but no PeerConnection exists")
            return

        candidate_data = msg.get("candidate")
        if candidate_data:
            candidate = RTCIceCandidate(
                candidate_data.get("candidate", ""),
                candidate_data.get("sdpMid"),
                candidate_data.get("sdpMLineIndex"),
            )
            await self._pc.addIceCandidate(candidate)
            logger.debug("Added ICE candidate")

    async def _wait_for_ice_gathering(self) -> None:
        """Wait for ICE gathering state to complete, or timeout after 3 seconds."""
        if self._pc is None or self._pc.iceGatheringState == "complete":
            return

        done = asyncio.Event()

        @self._pc.on("icegatheringstatechange")
        def on_state_change():
            if self._pc and self._pc.iceGatheringState == "complete":
                done.set()

        try:
            # Wait up to 10s for ICE gathering to complete to ensure STUN is collected
            await asyncio.wait_for(done.wait(), timeout=10.0)
            logger.info("ICE gathering complete")
        except asyncio.TimeoutError:
            logger.warning("ICE gathering timed out, sending partial SDP")

    # ------------------------------------------------------------------ #
    # PeerConnection and DataChannel event wiring
    # ------------------------------------------------------------------ #

    def _setup_pc_events(self) -> None:
        """Wire up events on the RTCPeerConnection."""
        assert self._pc is not None

        @self._pc.on("datachannel")
        def on_datachannel(channel):
            """Answerer receives the data channel here."""
            logger.info("Data channel received: %s", channel.label)
            self._channel = channel
            self._setup_channel_events(channel)
            
            # aiortc may not fire 'open' if it is already open when the event is attached
            if channel.readyState == "open":
                channel.bufferedAmountLowThreshold = 1024 * 1024
                self.channel_ready.set()
                if self._on_channel_ready is not None:
                    result = self._on_channel_ready()
                    if asyncio.iscoroutine(result):
                        asyncio.ensure_future(result)

        @self._pc.on("connectionstatechange")
        async def on_connection_state_change():
            state = self._pc.connectionState
            logger.info("Connection state: %s", state)
            if state in ("failed", "closed", "disconnected"):
                await self.close()
                if self._on_connection_closed is not None:
                    self._on_connection_closed()

    def _setup_channel_events(self, channel) -> None:
        """Wire up events on the data channel."""

        @channel.on("open")
        def on_open():
            logger.info("Data channel OPEN: %s", channel.label)
            # Set buffer threshold (e.g. 1MB)
            channel.bufferedAmountLowThreshold = 1024 * 1024
            self.channel_ready.set()
            if self._on_channel_ready is not None:
                result = self._on_channel_ready()
                if asyncio.iscoroutine(result):
                    asyncio.ensure_future(result)

        @channel.on("bufferedamountlow")
        def on_bufferedamountlow():
            self.buffer_low.set()

        @channel.on("message")
        def on_message(message):
            if self._on_message is not None:
                result = self._on_message(message)
                if asyncio.iscoroutine(result):
                    asyncio.ensure_future(result)

        @channel.on("close")
        def on_close():
            logger.info("Data channel CLOSED")
            if self._on_connection_closed is not None:
                self._on_connection_closed()

    # ------------------------------------------------------------------ #
    # Sending
    # ------------------------------------------------------------------ #

    def send(self, data: bytes | str) -> None:
        """Send data over the open data channel (synchronous, ignores backpressure)."""
        if self._channel is None:
            raise RuntimeError("Data channel not open yet")
        self._channel.send(data)

    async def send_with_backpressure(self, data: bytes | str) -> None:
        """Send data, waiting if the underlying send buffer is too full."""
        if self._channel is None:
            raise RuntimeError("Data channel not open yet")
        
        # If buffer is full, wait until it drains below threshold
        if self._channel.bufferedAmount > self._channel.bufferedAmountLowThreshold:
            self.buffer_low.clear()
            await self.buffer_low.wait()
            
        self._channel.send(data)

    # ------------------------------------------------------------------ #
    # Teardown
    # ------------------------------------------------------------------ #

    async def disconnect_peer(self) -> None:
        """Close only the WebRTC peer connection, keeping the signalling connection alive."""
        if self._pc is not None:
            await self._pc.close()
            self._pc = None
        self._channel = None
        self.channel_ready.clear()
        # Trigger the callback to update UI
        if self._on_connection_closed is not None:
            self._on_connection_closed()

    async def close(self) -> None:
        """Tear down the peer connection and signalling WebSocket."""
        if self._signalling_task is not None:
            self._signalling_task.cancel()
            self._signalling_task = None

        if self._pc is not None:
            await self._pc.close()
            self._pc = None

        if self._ws is not None:
            await self._ws.close()
            self._ws = None

        logger.info("Peer %s closed", self.peer_id)
