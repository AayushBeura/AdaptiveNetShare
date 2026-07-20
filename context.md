# AdaptiveNetShare — Free 2–3 Week Action Plan

Everything here uses free tiers, open-source tools, and no credit card required.

---

## Free Infrastructure (set up on Day 1)

**Signalling server** — Railway.app free tier or Render.com free tier. Both give you a persistent WebSocket server with no CC required. Alternatively, Fly.io has a free allowance with just an email signup.

**TURN/STUN relay** — Use Open Relay Project (openrelay.metered.ca) — completely free, no signup needed. It's a public TURN server used by thousands of WebRTC apps. This eliminates the need to host coturn yourself.

**Backend hosting** — Supabase free tier for any metadata/signalling state you need to persist, or just keep the signalling server fully stateless (recommended — easier to deploy for free).

---

## Tech Stack (all free, all open source)

- **Core language** — Python 3.11+ (fastest to build in, great networking libraries, runs on all desktop platforms)
- **P2P/WebRTC** — `aiortc` library (Python WebRTC implementation, handles ICE/STUN/TURN automatically)
- **UI** — `customtkinter` (desktop) or a simple web UI with Flask + HTML (runs in browser, works cross-platform)
- **Signalling server** — Python + `websockets` library, deployed on Render free tier
- **File hashing** — Python built-in `hashlib`
- **Concurrency** — Python `asyncio` + `aiofiles`

---

## Week 1 — Core Engine

### Days 1–2: Project scaffold and signalling server

Set up the repo. Build the signalling server first because everything else depends on it.

The signalling server is a WebSocket server with three responsibilities: register a peer with an ID, forward SDP offers between peers, and forward ICE candidates between peers. It holds zero file data and zero state beyond active connections. Write it in under 100 lines of Python using the `websockets` library. Deploy it to Render.com by connecting your GitHub repo — Render detects Python automatically and deploys for free with a public URL.

Test it by connecting two browser WebSocket clients and confirming messages pass through.

### Days 3–4: P2P connection via WebRTC (aiortc)

Install `aiortc` and build the peer module. Each peer on startup generates a UUID as its device ID. The connection flow is: Peer A sends an SDP offer to the signalling server with Peer B's ID as the target. The signalling server forwards it. Peer B sends back an SDP answer. Both sides exchange ICE candidates through the signalling server. Once ICE negotiation completes, the data channel opens directly between the two devices — or through the free TURN relay if NAT blocks direct connection.

Use `RTCPeerConnection` from aiortc with the Open Relay STUN/TURN servers configured. The STUN URL is `stun:openrelay.metered.ca:80` and the TURN URLs are `turn:openrelay.metered.ca:80`, `turn:openrelay.metered.ca:443`, and `turn:openrelay.metered.ca:443?transport=tcp`. This handles essentially all NAT types for free.

Test by connecting two terminals on the same machine first, then two machines on the same WiFi, then two machines on different networks.

### Day 5: Chunk transfer protocol

Design the message format as a simple Python dataclass serialized to JSON or `msgpack`. A chunk message contains: `type` (either "manifest", "chunk", "ack", "nack", or "done"), `transfer_id` (UUID), `chunk_index` (integer), `total_chunks` (integer), `sha256` (hex string), and `data` (base64-encoded bytes for chunks, or null for control messages).

The manifest message is sent first and contains the filename, total size, total chunks, chunk size, and the whole-file SHA-256. The receiver stores this, then requests chunks. The sender streams chunks over the data channel. The receiver verifies each chunk's hash, writes it to a temp file in order, and sends ACK. On NACK (bad hash), sender retransmits that chunk only. When all chunks are ACKed, receiver renames temp file to final filename and sends DONE.

Build this as a pure Python module with no UI yet. Test it by transferring a 100MB file between two terminals.

---

## Week 2 — Features and UI

### Days 6–7: Parallel chunking and resumable transfers

Replace the single sequential stream with a chunk queue. The sender maintains a queue of pending chunks and sends up to N chunks ahead without waiting for ACK (a sliding window, like TCP). Start with N=8 and make it configurable. This alone will multiply your throughput significantly.

For resumable transfers, write a checkpoint file to disk alongside the temp file. The checkpoint is a JSON file containing the transfer ID, manifest, and a set of received chunk indices. If the transfer is interrupted and restarted, the receiver loads the checkpoint, tells the sender which chunks it already has, and the sender skips those. The checkpoint file is deleted when the transfer completes.

### Days 8–9: LAN discovery (no internet needed for local transfers)

Add mDNS discovery using the `zeroconf` Python library (install with pip, no cost). On startup, each peer broadcasts a service record of type `_adaptivenetshare._tcp.local` with its device name, device ID, and the port it's listening on. Each peer also browses for the same service type and maintains a live list of discovered peers on the LAN. This means two devices on the same WiFi find each other instantly with no server involved at all. The signalling server is only needed for WAN connections.

### Days 10–12: Desktop UI

Build the UI with `customtkinter`. The main window has four sections. The top section shows this device's name and ID with a QR code or copyable ID string. The peers section lists discovered LAN peers and lets you add a WAN peer by ID. The transfers section shows active and completed transfers with a progress bar, speed readout, and pause/cancel buttons. The settings section has a folder picker for the default download location and a chunk concurrency slider (4 to 32).

All UI updates come from asyncio callbacks — run the WebRTC event loop in a background thread and use `root.after()` to push updates to the tkinter main thread.

---

## Week 3 — Polish, Security, and Evaluation

### Days 13–14: Security

On first launch, generate an Ed25519 keypair using Python's `cryptography` library and save it to a local config file. The public key acts as the device's permanent identity. When two peers connect for the first time, they exchange public keys over the already-encrypted WebRTC DTLS channel (WebRTC encrypts everything by default with DTLS-SRTP, so you get encryption for free). Store trusted device public keys in a local JSON file. On subsequent connections, verify the public key matches. This is Trust On First Use — the same model SSH uses. No certificates, no CA, no cost.

Add a permission flag per trusted device: `receive_only` or `send_and_receive`. Enforce this in the transfer handler.

### Days 15–16: Error handling and edge cases

Handle the common failure modes explicitly: signalling server unreachable (show clear error, retry with backoff), TURN relay fails (show error suggesting LAN usage), file disappears mid-transfer (send error message, clean up temp file), disk full on receiver (check available space before accepting manifest, reject with reason), and transfer interrupted (checkpoint is already handled — just make sure the UI reflects "Paused / Resume" correctly).

Add a transfer log that persists to disk so the user can see history across sessions.

### Days 17–18: Benchmarking and evaluation

Write a benchmark script that automates the following tests and outputs results to a CSV. Test 1: transfer a 10MB, 500MB, and 2GB file on LAN and record throughput and time. Test 2: simulate packet loss using the Python socket's timeout and measure chunk retransmit rate. Test 3: run 3 simultaneous transfers and measure aggregate throughput. Test 4: interrupt a transfer at 50% and resume — verify correctness and measure resume overhead.

Compare your results against plain SCP (if on Linux/macOS) or a shared folder copy to establish a baseline. Document the comparison in a table.

### Days 19–21: Testing on actual devices and bug fixes

Install on Windows, Linux/macOS, and an Android device (via a simple Flask web UI served from the Python process — Android can access it via browser on the same LAN). Fix any platform-specific issues. The most common ones will be firewall rules on Windows (prompt the user to allow through Windows Firewall) and path separator issues in filenames on Windows vs Unix.

---

## Free Services Summary

| Need | Free Solution | Signup required |
|---|---|---|
| Signalling server hosting | Render.com free tier | Email only |
| STUN server | openrelay.metered.ca | None |
| TURN relay | openrelay.metered.ca | None |
| Code hosting | GitHub | Email only |
| Metrics/logs | Python logging to file + optional Grafana Cloud free tier | Email only |

---

## File and Folder Structure for the IDE Agent

```
adaptivenetshare/
├── signalling/
│   └── server.py          # WebSocket signalling server (deploy to Render)
├── core/
│   ├── peer.py            # WebRTC peer connection, ICE, data channel
│   ├── chunker.py         # File splitting, hashing, chunk queue
│   ├── scheduler.py       # Sliding window, adaptive concurrency
│   ├── integrity.py       # SHA-256 verify, manifest handling
│   └── resume.py          # Checkpoint read/write
├── discovery/
│   └── mdns.py            # zeroconf LAN peer discovery
├── security/
│   ├── identity.py        # Ed25519 keypair generation and storage
│   └── trust.py           # Trusted device store
├── ui/
│   └── app.py             # customtkinter main window
├── benchmark/
│   └── run_benchmarks.py  # Automated test suite
├── config.py              # Constants: chunk size, STUN/TURN URLs, ports
├── main.py                # Entry point
└── requirements.txt       # aiortc, websockets, zeroconf, customtkinter,
                           # cryptography, aiofiles, msgpack
```

---

## Execution Order for the IDE Agent

Give your agent these tasks in sequence. Each one builds on the last and can be independently tested before moving on.

1. Scaffold the folder structure and `requirements.txt`
2. Build and test `signalling/server.py` locally on port 8765
3. Deploy signalling server to Render.com
4. Build `core/peer.py` with STUN/TURN config and test two-peer connection
5. Build `core/chunker.py` and `core/integrity.py` and test with a local file
6. Build `core/scheduler.py` with sliding window and test transfer speed
7. Build `core/resume.py` and test interrupted transfer recovery
8. Build `discovery/mdns.py` and test LAN peer discovery
9. Build `security/identity.py` and `security/trust.py`
10. Build `ui/app.py` wiring all modules together
11. Run `benchmark/run_benchmarks.py` and collect results
12. Cross-platform testing and bug fixes

Each step produces something you can run and verify, so you always know where you stand.