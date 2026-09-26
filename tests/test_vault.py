import os
import sys
import shutil
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.ring import ConsistentHashRing
from app.durability import ReedSolomonEC, ReplicationEngine
from app.storage_node import StorageNode
from app.metadata_catalog import MetadataCatalog
from app.repair_service import RepairService

def test_consistent_hash_ring():
    ring = ConsistentHashRing(vnodes_per_node=50)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    nodes = ring.get_nodes("test_file.png", 3)
    assert len(nodes) == 3
    assert set(nodes) == {"node-1", "node-2", "node-3"}

def test_erasure_coding_4_2_recovery():
    rs = ReedSolomonEC(data_shards=4, parity_shards=2)
    original_data = b"Hello Vault Distributed Storage System! Testing Reed Solomon 4+2."
    shards = rs.encode(original_data)
    assert len(shards) == 6

    # Simulate 2 node failures (lose shard 1 and shard 3)
    available = {idx: sdata for idx, sdata, chksum in shards if idx not in (1, 3)}
    assert len(available) == 4

    decoded = rs.decode(available, len(original_data))
    assert decoded == original_data

def test_storage_node_corruption_and_repair():
    temp_dir = tempfile.mkdtemp()
    try:
        catalog_dir = os.path.join(temp_dir, "catalog")
        nodes_dir = os.path.join(temp_dir, "nodes")
        os.makedirs(catalog_dir)
        os.makedirs(nodes_dir)

        # Setup 4 nodes
        nodes = {}
        ring = ConsistentHashRing()
        for nid in ["node-1", "node-2", "node-3", "node-4"]:
            nodes[nid] = StorageNode(nid, nodes_dir)
            ring.add_node(nid)

        catalog = MetadataCatalog(catalog_dir)
        repair = RepairService(nodes, catalog, ring)

        # Write replicated object
        key = "document.pdf"
        data = b"Vault Confidential PDF Document Payload Content"
        rep = ReplicationEngine(replicas=3)
        replica_chunks = rep.create_replicas(data)

        target_nodes = ["node-1", "node-2", "node-3"]
        chunk_manifest = []

        for idx, chunk_bytes, chksum in replica_chunks:
            for nid in target_nodes:
                nodes[nid].write_chunk(key, idx, chunk_bytes, chksum)
            chunk_manifest.append({"index": idx, "checksum": chksum, "nodes": list(target_nodes)})

        manifest = {
            "object_key": key,
            "size_bytes": len(data),
            "checksum": replica_chunks[0][2],
            "durability_policy": "replication",
            "policy_config": {"replicas": 3},
            "chunk_manifest": chunk_manifest,
            "created_at": 1000.0,
            "filename": "document.pdf"
        }
        catalog.put_object(manifest)

        # Corrupt node-1 disk block
        corrupted = nodes["node-1"].corrupt_chunk(key, 0)
        assert corrupted == True

        # Verify node-1 detects corruption
        check = nodes["node-1"].verify_chunk(key, 0)
        assert check["valid"] == False
        assert check["status"] == "CORRUPTED"

        # Run Auto-Repair
        report = repair.run_scrub_and_repair()
        assert report["repaired_count"] == 1

        # Verify node-1 chunk is now HEALTHY
        check_after = nodes["node-1"].verify_chunk(key, 0)
        assert check_after["valid"] == True
        assert check_after["status"] == "HEALTHY"

    finally:
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    test_consistent_hash_ring()
    test_erasure_coding_4_2_recovery()
    test_storage_node_corruption_and_repair()
    print("ALL BACKEND TESTS PASSED SUCCESSFULLY!")
