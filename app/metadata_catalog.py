import os
import json
import time
import threading
from typing import Dict, Optional, List

class MetadataCatalog:
    """
    Object Metadata Catalog & Write-Ahead Log (WAL) for Vault.
    Guarantees metadata consistency, transaction tracking, and recovery.
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.catalog_path = os.path.join(data_dir, "metadata_catalog.json")
        self.wal_path = os.path.join(data_dir, "wal.log")
        self.lock = threading.Lock()
        self.objects: Dict[str, Dict] = {}

        os.makedirs(data_dir, exist_ok=True)
        self._load_catalog()
        self._recover_from_wal()

    def _load_catalog(self):
        if os.path.exists(self.catalog_path):
            try:
                with open(self.catalog_path, "r", encoding="utf-8") as f:
                    self.objects = json.load(f)
            except Exception:
                self.objects = {}
        else:
            self.objects = {}

    def _flush_catalog(self):
        with open(self.catalog_path, "w", encoding="utf-8") as f:
            json.dump(self.objects, f, indent=2)

    def _append_wal(self, record: Dict):
        with open(self.wal_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _recover_from_wal(self):
        """Replay uncommitted WAL logs on startup."""
        if not os.path.exists(self.wal_path):
            return

        with open(self.wal_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    action = record.get("action")
                    key = record.get("key")
                    if action == "COMMIT_PUT":
                        self.objects[key] = record["manifest"]
                    elif action == "COMMIT_DELETE":
                        if key in self.objects:
                            del self.objects[key]
                except Exception:
                    pass
        self._flush_catalog()

    def put_object(self, manifest: Dict) -> bool:
        with self.lock:
            key = manifest["object_key"]
            # 1. Write-Ahead Log PREPARE
            wal_record = {
                "tx_id": f"tx_{int(time.time()*1000)}",
                "action": "PREPARE_PUT",
                "key": key,
                "timestamp": time.time()
            }
            self._append_wal(wal_record)

            # 2. Update memory state
            self.objects[key] = manifest

            # 3. Write-Ahead Log COMMIT
            wal_commit = {
                "tx_id": wal_record["tx_id"],
                "action": "COMMIT_PUT",
                "key": key,
                "manifest": manifest,
                "timestamp": time.time()
            }
            self._append_wal(wal_commit)

            # 4. Flush snapshot catalog
            self._flush_catalog()
            return True

    def get_object(self, object_key: str) -> Optional[Dict]:
        with self.lock:
            return self.objects.get(object_key)

    def delete_object(self, object_key: str) -> Optional[Dict]:
        with self.lock:
            if object_key not in self.objects:
                return None
            manifest = self.objects.pop(object_key)
            
            wal_record = {
                "tx_id": f"tx_{int(time.time()*1000)}",
                "action": "COMMIT_DELETE",
                "key": object_key,
                "timestamp": time.time()
            }
            self._append_wal(wal_record)
            self._flush_catalog()
            return manifest

    def list_objects(self) -> List[Dict]:
        with self.lock:
            return list(self.objects.values())

    def get_wal_entries(self, limit: int = 50) -> List[Dict]:
        """Fetch latest WAL entries for system telemetry stream."""
        if not os.path.exists(self.wal_path):
            return []
        entries = []
        with open(self.wal_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines[-limit:]:
                if line.strip():
                    try:
                        entries.append(json.loads(line))
                    except Exception:
                        pass
        return entries
