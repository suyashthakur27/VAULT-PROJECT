[README.md](https://github.com/user-attachments/files/32674035/README.md)
# Vault: Fault-Tolerant Distributed Object Storage System

Vault is an enterprise-grade distributed object storage system designed to store, replicate, retrieve, and repair large volumes of data across unreliable and independently failing storage nodes.

---

## 🌟 Key Features & Architecture

- **Consistent Hash Ring with Virtual Nodes ($V=100$)**: Distributes object keys across storage nodes with minimal chunk movement during node additions/decommissioning.
- **Configurable Durability Policies**:
  - **3x Replication**: Quorum Writes ($W=2$) and Quorum Reads ($R=2$) for strict consistency ($W + R > N$).
  - **Reed-Solomon Erasure Coding ($4+2$)**: $K=4$ data shards + $M=2$ parity shards over Galois Field $GF(2^8)$. Allows recovering original payloads under 2 simultaneous node crashes with only $1.5\times$ storage overhead compared to $3.0\times$ replication!
- **Write-Ahead Logging (WAL) Metadata Catalog**: Transactional safety (`PREPARE`, `COMMIT`, `ABORT`) preventing metadata corruption on unexpected server restarts.
- **Automated Self-Healing & Background Scrubbing**: Background scanner detects bit-rot data corruption via SHA256 checksum verification and reconstructs lost/corrupted chunks automatically via surviving replicas or Reed-Solomon decoding.
- **Chaos Engineering Lab**: Live control panel to simulate disk bit-rot corruption, node crashes, latency spikes, and observe real-time self-healing.
- **High-Tech Industrial Control Dashboard**: Live HTML5 topology canvas, real-time Server-Sent Events (SSE) telemetry stream, drag-and-drop object drive, and IOPs analytics.

---

## 📁 File Structure & Upload / Placement Matrix

When creating or deploying this application on a target machine, place all files exactly as organized below:

```
vault-storage/
├── backend/
│   ├── app/
│   │   ├── __init__.py           # Package marker
│   │   ├── main.py               # Gateway REST API & Real-Time Telemetry SSE Stream
│   │   ├── ring.py               # Consistent Hash Ring with Virtual Nodes
│   │   ├── durability.py         # Replication (3x) & Reed-Solomon Erasure Coding Engine
│   │   ├── storage_node.py       # Physical/Virtual Storage Disk Driver & Chaos Injector
│   │   ├── metadata_catalog.py   # Object Catalog & Write-Ahead Logging (WAL)
│   │   └── repair_service.py     # Background Scrubber & Auto-Repair Worker
│   ├── tests/
│   │   └── test_vault.py         # Comprehensive Automated Test Suite
│   ├── requirements.txt           # Python dependencies (fastapi, uvicorn, pydantic, python-multipart)
│   └── run.py                     # Entry point server launcher script
├── frontend/
│   ├── index.html                 # Single-Page Application (SPA) Storage Dashboard
│   ├── app.js                     # State manager, SSE client, and HTML5 Canvas topology graph
│   └── style.css                  # Industrial Dark Cyber Theme & Glassmorphism styling
├── data/                          # Virtual Node storage disks & metadata WAL logs (auto-generated)
└── README.md                      # Complete Documentation & Usage Guide
```

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.10 or higher installed.

### 1. Installation
Open your terminal inside `vault-storage/backend` and run:

```bash
cd backend
python -m pip install -r requirements.txt
```

### 2. Run Automated Test Suite
Verify core storage engine, hash ring, erasure coding math, and self-healing:

```bash
python tests/test_vault.py
```

### 3. Launch Vault Server
Start the REST API Gateway and static frontend server:

```bash
python run.py
```

The application will start on **`http://127.0.0.1:8000`**.

---

## 💻 How to Use the Dashboard UI

1. Open your browser to `http://127.0.0.1:8000`.
2. **Vault Drive (File Manager)**: Select a file, pick either **3x Replication** or **Reed-Solomon (4+2)** policy, and click **Upload**.
3. **Consistent Hash Ring Map**: Watch the real-time canvas visualize virtual ring positions and storage node health.
4. **Chaos Engineering Lab**:
   - Click **Crash Node** to mark a storage node offline.
   - Select an object and target node, then click **Inject Bit-Rot Corruption** to corrupt a disk block on purpose.
   - Click **Auto-Scrub & Repair** in the header or watch the background engine automatically detect bit-rot and repair lost chunks!
5. **Write-Ahead Log (WAL) Stream**: Inspect real-time transaction logs and IOPs metrics in the telemetry box.

---

## 📡 REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/objects` | Upload file (`object_key`, `durability_policy`, `file`) |
| `GET` | `/api/objects/{key}` | Download file with Quorum Read & transparent repair |
| `DELETE` | `/api/objects/{key}` | Delete file & purge chunks across nodes |
| `GET` | `/api/objects` | List stored objects & chunk health metrics |
| `GET` | `/api/cluster/nodes` | Get storage node health & disk utilization |
| `POST` | `/api/cluster/nodes/{id}/status` | Toggle node state (`HEALTHY`, `DEGRADED`, `OFFLINE`) |
| `POST` | `/api/cluster/inject-fault` | Corrupt disk block on target node (Bit-Rot) |
| `POST` | `/api/cluster/scrub` | Trigger manual integrity scan and auto-repair cycle |
| `POST` | `/api/cluster/nodes` | Dynamically add new node to consistent hash ring |
| `GET` | `/api/telemetry/stream` | Server-Sent Events (SSE) live telemetry stream |
