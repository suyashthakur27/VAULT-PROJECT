import os
import json
import time
import hashlib
from typing import Dict, Tuple, Optional, List

class StorageNode:
    """
    Simulated Distributed Storage Node representing an independent disk/server.
    Manages chunk persistence, checksum validation, and chaos fault injection.
    """
    def __init__(self, node_id: str, base_dir: str):
        self.node_id = node_id
        self.node_dir = os.path.join(base_dir, node_id)
        self.chunks_dir = os.path.join(self.node_dir, "chunks")
        os.makedirs(self.chunks_dir, exist_ok=True)

        # Fault Injection States
        self.status = "HEALTHY"  # HEALTHY, DEGRADED, OFFLINE
        self.latency_ms = 0
        self.corrupted_count = 0

    def _get_chunk_paths(self, object_key: str, chunk_index: int) -> Tuple[str, str]:
        # Sanitize object key for filesystem safety
        safe_key = hashlib.md5(object_key.encode()).hexdigest()
        data_path = os.path.join(self.chunks_dir, f"{safe_key}_chk_{chunk_index}.bin")
        meta_path = os.path.join(self.chunks_dir, f"{safe_key}_chk_{chunk_index}.meta")
        return data_path, meta_path

    def write_chunk(self, object_key: str, chunk_index: int, payload: bytes, checksum: str) -> bool:
        if self.status == "OFFLINE":
            raise ConnectionError(f"Node {self.node_id} is OFFLINE!")

        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)

        data_path, meta_path = self._get_chunk_paths(object_key, chunk_index)

        with open(data_path, "wb") as f:
            f.write(payload)

        meta = {
            "node_id": self.node_id,
            "object_key": object_key,
            "chunk_index": chunk_index,
            "checksum": checksum,
            "size": len(payload),
            "written_at": time.time()
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        return True

    def read_chunk(self, object_key: str, chunk_index: int) -> Tuple[bytes, str]:
        if self.status == "OFFLINE":
            raise ConnectionError(f"Node {self.node_id} is OFFLINE!")

        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)

        data_path, meta_path = self._get_chunk_paths(object_key, chunk_index)
        if not os.path.exists(data_path) or not os.path.exists(meta_path):
            raise FileNotFoundError(f"Chunk {chunk_index} of {object_key} not found on node {self.node_id}")

        with open(data_path, "rb") as f:
            payload = f.read()

        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        return payload, meta["checksum"]

    def delete_chunk(self, object_key: str, chunk_index: int) -> bool:
        data_path, meta_path = self._get_chunk_paths(object_key, chunk_index)
        removed = False
        if os.path.exists(data_path):
            os.remove(data_path)
            removed = True
        if os.path.exists(meta_path):
            os.remove(meta_path)
        return removed

    def corrupt_chunk(self, object_key: str, chunk_index: int) -> bool:
        """Simulate Hardware Bit-Rot / Data Corruption by modifying payload bytes on disk."""
        data_path, meta_path = self._get_chunk_paths(object_key, chunk_index)
        if not os.path.exists(data_path):
            return False

        with open(data_path, "r+b") as f:
            content = bytearray(f.read())
            if len(content) > 0:
                # Flip bits in the first byte
                content[0] ^= 0xFF
                f.seek(0)
                f.write(content)
                f.truncate()
        
        self.corrupted_count += 1
        return True

    def verify_chunk(self, object_key: str, chunk_index: int) -> Dict:
        """Background Scrubber verification tool: checks stored bytes against manifest SHA256."""
        if self.status == "OFFLINE":
            return {"node_id": self.node_id, "status": "OFFLINE", "valid": False}

        data_path, meta_path = self._get_chunk_paths(object_key, chunk_index)
        if not os.path.exists(data_path) or not os.path.exists(meta_path):
            return {"node_id": self.node_id, "status": "MISSING", "valid": False}

        with open(data_path, "rb") as f:
            payload = f.read()

        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        actual_checksum = hashlib.sha256(payload).hexdigest()
        is_valid = (actual_checksum == meta["checksum"])

        return {
            "node_id": self.node_id,
            "object_key": object_key,
            "chunk_index": chunk_index,
            "expected_checksum": meta["checksum"],
            "actual_checksum": actual_checksum,
            "valid": is_valid,
            "size": len(payload),
            "status": "CORRUPTED" if not is_valid else "HEALTHY"
        }

    def list_all_chunks(self) -> List[Dict]:
        """Scan node disk for stored chunks."""
        chunks = []
        if not os.path.exists(self.chunks_dir):
            return chunks

        for filename in os.listdir(self.chunks_dir):
            if filename.endswith(".meta"):
                meta_path = os.path.join(self.chunks_dir, filename)
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                        chunks.append(meta)
                except Exception:
                    pass
        return chunks

    def get_metrics(self) -> Dict:
        """Get disk utilization and health status."""
        total_bytes = 0
        chunk_count = 0

        if os.path.exists(self.chunks_dir):
            for fname in os.listdir(self.chunks_dir):
                fpath = os.path.join(self.chunks_dir, fname)
                if os.path.isfile(fpath):
                    total_bytes += os.path.getsize(fpath)
                    if fname.endswith(".bin"):
                        chunk_count += 1

        return {
            "node_id": self.node_id,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "chunk_count": chunk_count,
            "used_bytes": total_bytes,
            "corrupted_count": self.corrupted_count
        }
