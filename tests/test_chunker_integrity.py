"""
Smoke test: verify FileSender, FileReceiver, integrity, and message
serialization work end-to-end with a real file.
"""

import os
import tempfile
from pathlib import Path

from adaptivenetshare.core.integrity import hash_file, hash_bytes, verify_file, verify_chunk
from adaptivenetshare.core.chunker import FileSender, FileReceiver
from adaptivenetshare.core.messages import serialize, deserialize, Manifest, ChunkMessage, AckMessage, DoneMessage

# ---- Create a test file with known content ----
TEST_SIZE = 256 * 1024  # 256 KB
test_dir = Path(tempfile.mkdtemp(prefix="ans_test_"))
test_file = test_dir / "testfile.bin"
test_file.write_bytes(os.urandom(TEST_SIZE))

print(f"Test file: {test_file}  ({TEST_SIZE} bytes)")
file_hash = hash_file(test_file)
print(f"SHA-256: {file_hash}")

# ---- Test FileSender ----
sender = FileSender(test_file, chunk_size=65536)
manifest = sender.get_manifest()
print(f"\nManifest: {manifest.filename}, {manifest.total_chunks} chunks, hash={manifest.sha256[:16]}...")
assert manifest.file_size == TEST_SIZE
assert manifest.total_chunks == 4  # 256KB / 64KB
assert manifest.sha256 == file_hash
print("[OK] FileSender manifest OK")

# ---- Test message serialization ----
raw = serialize(manifest)
m2 = deserialize(raw)
assert isinstance(m2, Manifest)
assert m2.filename == manifest.filename
assert m2.sha256 == manifest.sha256
print("[OK] Manifest serialize/deserialize OK")

# ---- Test FileReceiver ----
recv_dir = test_dir / "received"
receiver = FileReceiver(manifest, download_dir=recv_dir)

for i in range(sender.total_chunks):
    chunk = sender.get_chunk(i)
    
    # Serialize round-trip
    raw_chunk = serialize(chunk)
    chunk2 = deserialize(raw_chunk)
    assert isinstance(chunk2, ChunkMessage)
    assert chunk2.chunk_index == i
    
    # Verify chunk hash
    assert verify_chunk(chunk.data, chunk.sha256), f"Chunk {i} hash verify failed"
    
    # Write to receiver
    ok = receiver.receive_chunk(chunk)
    assert ok, f"Chunk {i} receive failed"

print(f"[OK] All {sender.total_chunks} chunks received and verified")

assert receiver.is_complete()
final_path = receiver.finalize()
print(f"[OK] File finalized: {final_path}")

# ---- Verify whole file ----
assert verify_file(final_path, file_hash), "Whole-file hash mismatch!"
print("[OK] Whole-file SHA-256 verified")

# ---- Test ACK/Done serialization ----
ack = AckMessage(transfer_id=manifest.transfer_id, chunk_index=2)
raw_ack = serialize(ack)
ack2 = deserialize(raw_ack)
assert ack2.chunk_index == 2
print("[OK] AckMessage serialize/deserialize OK")

done = DoneMessage(transfer_id=manifest.transfer_id)
raw_done = serialize(done)
done2 = deserialize(raw_done)
assert done2.transfer_id == manifest.transfer_id
print("[OK] DoneMessage serialize/deserialize OK")

# ---- Cleanup ----
import shutil
shutil.rmtree(test_dir)
print(f"\n{'='*50}")
print("ALL TESTS PASSED [OK]")
