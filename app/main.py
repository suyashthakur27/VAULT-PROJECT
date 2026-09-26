import os
import time
import json
import asyncio
import hashlib
from typing import Dict, List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, BackgroundTasks
from fastapi.responses import Response, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .ring import ConsistentHashRing
from .durability import ReedSolomonEC, ReplicationEngine
from .storage_node import StorageNode
from .metadata_catalog import MetadataCatalog
from .repair_service import RepairService

# App Initialization
app = FastAPI(
    title="Vault: Fault-Tolerant Distributed Object Storage System",
    description="High-availability distributed object storage with auto-healing, replication, erasure coding, and chaos lab.",
    version="1.0.0"
)

# Enable CORS for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
NODES_DIR = os.path.join(DATA_DIR, "nodes")
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")

# Initialize Storage Cluster Components
hash_ring = ConsistentHashRing(vnodes_per_node=100)
metadata_catalog = MetadataCatalog(DATA_DIR)
storage_nodes: Dict[str, StorageNode] = {}

# Provision Default Cluster (6 Nodes)
INITIAL_NODES = ["node-1", "node-2", "node-3", "node-4", "node-5", "node-6"]
for n_id in INITIAL_NODES:
    storage_nodes[n_id] = StorageNode(n_id, NODES_DIR)
    hash_ring.add_node(n_id)

repair_service = RepairService(storage_nodes, metadata_catalog, hash_ring)

# Telemetry Counter
iops_counter = {"reads": 0, "writes": 0, "deletes": 0}


# ==========================================
# Pydantic Request Models
# ==========================================
class NodeStatusRequest(BaseModel):
    status: str  # HEALTHY, DEGRADED, OFFLINE
    latency_ms: Optional[int] = 0

class InjectFaultRequest(BaseModel):
    object_key: str
    chunk_index: int
    node_id: str

class AddNodeRequest(BaseModel):
    node_id: str


# ==========================================
# REST API Endpoints
# ==========================================

@app.post("/api/objects")
async def upload_object(
    object_key: str = Form(...),
    durability_policy: str = Form("replication"),  # "replication" or "erasure_coding"
    file: UploadFile = File(...)
):
    """
    Upload file object to distributed storage cluster.
    Applies consistent hash ring placement and selected durability policy.
    """
    iops_counter["writes"] += 1
    payload = await file.read()
    orig_size = len(payload)
    file_checksum = hashlib.sha256(payload).hexdigest()

    chunk_manifest = []

    if durability_policy == "replication":
        replicas_cnt = 3
        rep = ReplicationEngine(replicas=replicas_cnt)
        replica_chunks = rep.create_replicas(payload)

        # Use Hash Ring to get target physical nodes for key
        target_nodes = hash_ring.get_nodes(object_key, replicas_cnt)
        if len(target_nodes) < replicas_cnt:
            # Fallback to any active node if ring has fewer physical nodes
            target_nodes = list(storage_nodes.keys())[:replicas_cnt]

        for idx, chunk_data, chksum in replica_chunks:
            written_nodes = []
            # Write chunk to assigned replica nodes
            for n_id in target_nodes:
                node = storage_nodes.get(n_id)
                if node and node.status != "OFFLINE":
                    try:
                        node.write_chunk(object_key, idx, chunk_data, chksum)
                        written_nodes.append(n_id)
                    except Exception as e:
                        pass

            if not written_nodes:
                raise HTTPException(status_code=500, detail=f"Failed to write replica chunk {idx} to any available node.")

            chunk_manifest.append({
                "index": idx,
                "checksum": chksum,
                "nodes": written_nodes
            })

        policy_config = {"replicas": replicas_cnt, "write_quorum": 2, "read_quorum": 2}

    elif durability_policy == "erasure_coding":
        k_data, m_parity = 4, 2
        rs = ReedSolomonEC(data_shards=k_data, parity_shards=m_parity)
        shards = rs.encode(payload)

        target_nodes = hash_ring.get_nodes(object_key, k_data + m_parity)
        if len(target_nodes) < (k_data + m_parity):
            # Fallback if not enough distinct nodes
            target_nodes = (list(storage_nodes.keys()) * 2)[:(k_data + m_parity)]

        for idx, shard_data, chksum in shards:
            target_node_id = target_nodes[idx % len(target_nodes)]
            node = storage_nodes.get(target_node_id)
            
            if not node or node.status == "OFFLINE":
                # Fallback to next healthy node
                healthy = [nid for nid, n in storage_nodes.items() if n.status == "HEALTHY"]
                if healthy:
                    target_node_id = healthy[idx % len(healthy)]
                    node = storage_nodes[target_node_id]

            if not node or node.status == "OFFLINE":
                raise HTTPException(status_code=500, detail=f"No healthy node available for shard {idx}")

            node.write_chunk(object_key, idx, shard_data, chksum)
            chunk_manifest.append({
                "index": idx,
                "checksum": chksum,
                "nodes": [target_node_id]
            })

        policy_config = {"data_shards": k_data, "parity_shards": m_parity}

    else:
        raise HTTPException(status_code=400, detail="Invalid durability policy. Choose 'replication' or 'erasure_coding'")

    manifest = {
        "object_key": object_key,
        "size_bytes": orig_size,
        "checksum": file_checksum,
        "durability_policy": durability_policy,
        "policy_config": policy_config,
        "chunk_manifest": chunk_manifest,
        "created_at": time.time(),
        "filename": file.filename or object_key
    }

    metadata_catalog.put_object(manifest)

    return {
        "message": f"Object '{object_key}' uploaded successfully",
        "manifest": manifest
    }


@app.get("/api/objects/{object_key:path}")
async def download_object(object_key: str, background_tasks: BackgroundTasks):
    """
    Retrieve object with Quorum Verification & Transparent Self-Healing on Read.
    """
    iops_counter["reads"] += 1
    obj = metadata_catalog.get_object(object_key)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found")

    policy = obj["durability_policy"]
    chunks = obj["chunk_manifest"]
    orig_size = obj["size_bytes"]

    if policy == "replication":
        rep = ReplicationEngine()
        read_replica_chunks = []
        corrupted_detected = False

        for chk in chunks:
            c_idx = chk["index"]
            expected_chksum = chk["checksum"]
            nodes_list = chk["nodes"]

            for n_id in nodes_list:
                node = storage_nodes.get(n_id)
                if node and node.status != "OFFLINE":
                    try:
                        res = node.verify_chunk(object_key, c_idx)
                        if res["valid"]:
                            data, _ = node.read_chunk(object_key, c_idx)
                            read_replica_chunks.append((data, expected_chksum))
                            break
                        else:
                            corrupted_detected = True
                    except Exception:
                        pass

        if not read_replica_chunks:
            raise HTTPException(status_code=500, detail="All replicas unavailable or corrupted!")

        payload, consensus_hash = rep.resolve_quorum_read(read_replica_chunks)

        if corrupted_detected:
            # Trigger asynchronous background repair
            background_tasks.add_task(repair_service.run_scrub_and_repair)

        return Response(
            content=payload,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{obj.get("filename", object_key)}"'}
        )

    elif policy == "erasure_coding":
        p_cfg = obj.get("policy_config", {})
        k_data = p_cfg.get("data_shards", 4)
        m_parity = p_cfg.get("parity_shards", 2)
        rs = ReedSolomonEC(data_shards=k_data, parity_shards=m_parity)

        available_shards: Dict[int, bytes] = {}
        corrupted_detected = False

        for chk in chunks:
            c_idx = chk["index"]
            n_id = chk["nodes"][0] if chk["nodes"] else None
            node = storage_nodes.get(n_id)

            if node and node.status != "OFFLINE":
                res = node.verify_chunk(object_key, c_idx)
                if res["valid"]:
                    try:
                        data, _ = node.read_chunk(object_key, c_idx)
                        available_shards[c_idx] = data
                    except Exception:
                        pass
                else:
                    corrupted_detected = True

        if len(available_shards) < k_data:
            raise HTTPException(status_code=500, detail=f"Erasure Coding failure: insufficient surviving shards ({len(available_shards)}/{k_data})")

        payload = rs.decode(available_shards, orig_size)

        if corrupted_detected:
            background_tasks.add_task(repair_service.run_scrub_and_repair)

        return Response(
            content=payload,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{obj.get("filename", object_key)}"'}
        )


@app.delete("/api/objects/{object_key:path}")
async def delete_object(object_key: str):
    """Delete object chunks across all storage nodes."""
    iops_counter["deletes"] += 1
    obj = metadata_catalog.delete_object(object_key)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found")

    for chk in obj.get("chunk_manifest", []):
        c_idx = chk["index"]
        for n_id in chk.get("nodes", []):
            node = storage_nodes.get(n_id)
            if node:
                try:
                    node.delete_chunk(object_key, c_idx)
                except Exception:
                    pass

    return {"message": f"Object '{object_key}' deleted successfully"}


@app.get("/api/objects")
async def list_objects():
    """List all stored objects and chunk distribution state."""
    objects = metadata_catalog.list_objects()
    detailed_objects = []

    for obj in objects:
        key = obj["object_key"]
        chunks = obj["chunk_manifest"]
        healthy_count = 0
        corrupted_count = 0
        missing_count = 0

        for chk in chunks:
            c_idx = chk["index"]
            nodes = chk.get("nodes", [])
            for n_id in nodes:
                node = storage_nodes.get(n_id)
                if not node or node.status == "OFFLINE":
                    missing_count += 1
                else:
                    res = node.verify_chunk(key, c_idx)
                    if res["valid"]:
                        healthy_count += 1
                    else:
                        corrupted_count += 1

        detailed_objects.append({
            **obj,
            "health_summary": {
                "healthy_chunks": healthy_count,
                "corrupted_chunks": corrupted_count,
                "missing_chunks": missing_count
            }
        })

    return detailed_objects


# ==========================================
# Cluster Topology & Fault Injection APIs
# ==========================================

@app.get("/api/cluster/nodes")
async def get_nodes():
    """Get metrics and statuses for all storage nodes."""
    nodes_info = []
    for n_id, node in sorted(storage_nodes.items()):
        nodes_info.append(node.get_metrics())
    
    return {
        "nodes": nodes_info,
        "ring_topology": hash_ring.get_topology()
    }


@app.post("/api/cluster/nodes/{node_id}/status")
async def update_node_status(node_id: str, req: NodeStatusRequest):
    """Toggle storage node status (HEALTHY, DEGRADED, OFFLINE) or inject latency."""
    node = storage_nodes.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Storage node not found")

    node.status = req.status
    node.latency_ms = req.latency_ms or 0
    return {"message": f"Node {node_id} status updated to {req.status}", "metrics": node.get_metrics()}


@app.post("/api/cluster/inject-fault")
async def inject_fault(req: InjectFaultRequest):
    """Inject Bit Rot Data Corruption into a specific chunk on a node."""
    node = storage_nodes.get(req.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    corrupted = node.corrupt_chunk(req.object_key, req.chunk_index)
    if not corrupted:
        raise HTTPException(status_code=400, detail="Chunk not found on target node")

    return {
        "message": f"Bit-Rot data corruption injected into object '{req.object_key}' chunk {req.chunk_index} on {req.node_id}",
        "node_id": req.node_id
    }


@app.post("/api/cluster/nodes")
async def add_node(req: AddNodeRequest):
    """Dynamically provision a new storage node and register on hash ring."""
    n_id = req.node_id
    if n_id in storage_nodes:
        raise HTTPException(status_code=400, detail="Node already exists")

    storage_nodes[n_id] = StorageNode(n_id, NODES_DIR)
    hash_ring.add_node(n_id)

    return {"message": f"Node {n_id} added dynamically to cluster hash ring.", "metrics": storage_nodes[n_id].get_metrics()}


@app.post("/api/cluster/scrub")
async def trigger_scrub():
    """Trigger manual integrity scan and auto-repair cycle."""
    result = repair_service.run_scrub_and_repair()
    return {"message": "Integrity scrub and auto-repair cycle completed.", "report": result}


# ==========================================
# Real-Time Telemetry SSE Stream
# ==========================================

@app.get("/api/telemetry/stream")
async def telemetry_stream():
    """Server-Sent Events (SSE) streaming live metrics, IOPs, WAL events, and node health."""
    async def event_generator():
        while True:
            nodes_metrics = [n.get_metrics() for n in storage_nodes.values()]
            total_used_bytes = sum(m["used_bytes"] for m in nodes_metrics)
            total_chunks = sum(m["chunk_count"] for m in nodes_metrics)
            corrupted_chunks = sum(m["corrupted_count"] for m in nodes_metrics)
            offline_nodes = sum(1 for m in nodes_metrics if m["status"] == "OFFLINE")

            data = {
                "timestamp": time.time(),
                "iops": iops_counter,
                "cluster": {
                    "total_nodes": len(storage_nodes),
                    "active_nodes": len(storage_nodes) - offline_nodes,
                    "offline_nodes": offline_nodes,
                    "total_used_bytes": total_used_bytes,
                    "total_chunks": total_chunks,
                    "corrupted_chunks": corrupted_chunks,
                },
                "nodes": nodes_metrics,
                "wal_log": metadata_catalog.get_wal_entries(limit=10),
                "last_repair": repair_service.repair_history[-1] if repair_service.repair_history else None
            }

            yield f"data: {json.dumps(data)}\n\n"
            await asyncio.sleep(1.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# Mount static frontend app at root '/'
if os.path.exists(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
