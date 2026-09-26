import time
import logging
from typing import Dict, List, Tuple, Set
from .durability import ReedSolomonEC, ReplicationEngine
from .storage_node import StorageNode
from .metadata_catalog import MetadataCatalog
from .ring import ConsistentHashRing

logger = logging.getLogger("vault.repair")

class RepairService:
    """
    Automated Background Integrity Scrubber & Auto-Repair Worker for Vault.
    Monitors node health, detects bit-rot data corruption, and reconstructs lost/corrupted
    data chunks using Replication Quorum or Reed-Solomon Erasure Coding.
    """
    def __init__(
        self,
        nodes: Dict[str, StorageNode],
        catalog: MetadataCatalog,
        ring: ConsistentHashRing
    ):
        self.nodes = nodes
        self.catalog = catalog
        self.ring = ring
        self.last_scrub_time = 0
        self.repair_history: List[Dict] = []

    def scrub_all() -> Dict:
        pass  # Signature placeholder, implementation below

    def run_scrub_and_repair(self) -> Dict:
        """
        Scan all stored objects, identify missing/corrupted chunks, and execute auto-repair.
        """
        scan_results = []
        repaired_count = 0
        failed_count = 0
        start_time = time.time()

        objects = self.catalog.list_objects()

        for obj in objects:
            key = obj["object_key"]
            policy = obj["durability_policy"]
            policy_config = obj.get("policy_config", {})
            chunks = obj.get("chunk_manifest", [])

            obj_status = {
                "object_key": key,
                "durability_policy": policy,
                "total_chunks": len(chunks),
                "corrupted_chunks": [],
                "missing_chunks": [],
                "healthy_chunks": [],
                "repaired": False
            }

            if policy == "replication":
                # Check replicas for object
                for chk in chunks:
                    c_idx = chk["index"]
                    target_nodes = chk["nodes"]
                    checksum = chk["checksum"]

                    healthy_nodes = []
                    corrupted_nodes = []
                    missing_nodes = []

                    for n_id in target_nodes:
                        node = self.nodes.get(n_id)
                        if not node or node.status == "OFFLINE":
                            missing_nodes.append(n_id)
                            continue

                        res = node.verify_chunk(key, c_idx)
                        if res["valid"]:
                            healthy_nodes.append(n_id)
                        elif res["status"] == "CORRUPTED":
                            corrupted_nodes.append(n_id)
                        else:
                            missing_nodes.append(n_id)

                    if corrupted_nodes or missing_nodes:
                        obj_status["corrupted_chunks"].extend(corrupted_nodes)
                        obj_status["missing_chunks"].extend(missing_nodes)

                        # Attempt Replication Repair
                        if healthy_nodes:
                            source_node = self.nodes[healthy_nodes[0]]
                            payload, _ = source_node.read_chunk(key, c_idx)

                            # Repair corrupted or missing nodes
                            repaired_nodes = list(healthy_nodes)
                            bad_nodes = corrupted_nodes + missing_nodes

                            for bad_n_id in bad_nodes:
                                # Pick a healthy node target (if bad_n_id is offline, pick another available node)
                                target_n_id = bad_n_id
                                target_node = self.nodes.get(target_n_id)
                                if not target_node or target_node.status == "OFFLINE":
                                    # Find an available healthy node not currently in target_nodes
                                    available = [
                                        nid for nid, n in self.nodes.items()
                                        if n.status == "HEALTHY" and nid not in repaired_nodes
                                    ]
                                    if available:
                                        target_n_id = available[0]
                                        target_node = self.nodes[target_n_id]
                                    else:
                                        target_node = None

                                if target_node and target_node.status != "OFFLINE":
                                    target_node.write_chunk(key, c_idx, payload, checksum)
                                    repaired_nodes.append(target_n_id)

                            # Update manifest node list
                            chk["nodes"] = list(set(repaired_nodes))
                            repaired_count += 1
                            obj_status["repaired"] = True
                        else:
                            failed_count += 1
                    else:
                        obj_status["healthy_chunks"].extend(healthy_nodes)

            elif policy == "erasure_coding":
                # Reed-Solomon Erasure Coding Check
                data_shards_cnt = policy_config.get("data_shards", 4)
                parity_shards_cnt = policy_config.get("parity_shards", 2)
                rs = ReedSolomonEC(data_shards=data_shards_cnt, parity_shards=parity_shards_cnt)

                available_shards: Dict[int, bytes] = {}
                available_checksums: Dict[int, str] = {}
                missing_shard_indices: Set[int] = set()

                for chk in chunks:
                    c_idx = chk["index"]
                    n_id = chk["nodes"][0] if chk["nodes"] else None
                    checksum = chk["checksum"]

                    node = self.nodes.get(n_id)
                    if not node or node.status == "OFFLINE":
                        missing_shard_indices.add(c_idx)
                        obj_status["missing_chunks"].append(f"shard_{c_idx}")
                        continue

                    res = node.verify_chunk(key, c_idx)
                    if res["valid"]:
                        payload, _ = node.read_chunk(key, c_idx)
                        available_shards[c_idx] = payload
                        available_checksums[c_idx] = checksum
                        obj_status["healthy_chunks"].append(f"shard_{c_idx}")
                    else:
                        missing_shard_indices.add(c_idx)
                        obj_status["corrupted_chunks"].append(f"shard_{c_idx}")

                # If missing/corrupted shards exist, attempt RS Reconstruction
                if missing_shard_indices:
                    if len(available_shards) >= data_shards_cnt:
                        # Reconstruct full payload
                        orig_size = obj["size_bytes"]
                        reconstructed_payload = rs.decode(available_shards, orig_size)
                        
                        # Re-encode all shards
                        reencoded_shards = rs.encode(reconstructed_payload)

                        # Write missing/corrupted shards back to healthy storage nodes
                        healthy_nodes_list = [nid for nid, n in self.nodes.items() if n.status == "HEALTHY"]
                        
                        for idx, payload_bytes, chksum in reencoded_shards:
                            if idx in missing_shard_indices:
                                chk_meta = chunks[idx]
                                orig_node_id = chk_meta["nodes"][0] if chk_meta["nodes"] else None
                                target_node_id = orig_node_id

                                target_node = self.nodes.get(target_node_id)
                                if not target_node or target_node.status == "OFFLINE":
                                    # Pick replacement healthy node
                                    used_nodes = {c["nodes"][0] for c in chunks if c["nodes"]}
                                    cand = [nid for nid in healthy_nodes_list if nid not in used_nodes]
                                    if cand:
                                        target_node_id = cand[0]
                                        target_node = self.nodes[target_node_id]

                                if target_node and target_node.status != "OFFLINE":
                                    target_node.write_chunk(key, idx, payload_bytes, chksum)
                                    chk_meta["nodes"] = [target_node_id]
                                    chk_meta["checksum"] = chksum

                        repaired_count += 1
                        obj_status["repaired"] = True
                    else:
                        failed_count += 1

            if obj_status["repaired"]:
                self.catalog.put_object(obj)

            scan_results.append(obj_status)

        self.last_scrub_time = time.time()
        result_summary = {
            "timestamp": self.last_scrub_time,
            "duration_ms": round((time.time() - start_time) * 1000, 2),
            "objects_scanned": len(objects),
            "repaired_count": repaired_count,
            "failed_count": failed_count,
            "details": scan_results
        }

        self.repair_history.append(result_summary)
        return result_summary
