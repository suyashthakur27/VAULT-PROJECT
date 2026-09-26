// Vault UI State Manager & Real-Time SSE Controller
let state = {
    nodes: [],
    objects: [],
    iops: { reads: 0, writes: 0, deletes: 0 },
    wal_log: [],
    last_repair: null,
    topology: null
};

// Canvas Ring Renderer State
const canvas = document.getElementById('topology-canvas');
const ctx = canvas.getContext('2d');
let animationFrameId = null;
let dataRays = [];

// Initialize Dashboard
document.addEventListener('DOMContentLoaded', () => {
    initSSE();
    fetchObjects();
    fetchNodes();
    startCanvasLoop();

    document.getElementById('upload-form').addEventListener('submit', handleUpload);
});

// Real-Time Telemetry Stream via SSE
function initSSE() {
    const evtSource = new EventSource('/api/telemetry/stream');

    evtSource.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            state.iops = data.iops;
            state.nodes = data.nodes;
            state.wal_log = data.wal_log;
            state.last_repair = data.last_repair;

            updateDashboardUI(data);
        } catch (err) {
            console.error('SSE Error:', err);
        }
    };

    evtSource.onerror = (err) => {
        console.warn('SSE Disconnected. Retrying...', err);
    };
}

// Fetch Stored Objects List
async function fetchObjects() {
    try {
        const res = await fetch('/api/objects');
        if (res.ok) {
            state.objects = await res.json();
            renderObjectsTable();
            populateChaosSelects();
        }
    } catch (e) {
        console.error('Failed to fetch objects', e);
    }
}

// Fetch Storage Nodes Topology
async function fetchNodes() {
    try {
        const res = await fetch('/api/cluster/nodes');
        if (res.ok) {
            const data = await res.json();
            state.nodes = data.nodes;
            state.topology = data.ring_topology;
            renderNodesControlList();
            populateChaosSelects();
        }
    } catch (e) {
        console.error('Failed to fetch nodes', e);
    }
}

// Update Header & Stat Counters
function updateDashboardUI(data) {
    // IOPs
    document.getElementById('iops-metrics').innerText = `${data.iops.reads} R / ${data.iops.writes} W`;

    // Active Nodes
    const activeCnt = data.cluster.active_nodes;
    const totalCnt = data.cluster.total_nodes;
    document.getElementById('stat-active-nodes').innerText = `${activeCnt}/${totalCnt}`;

    // Cluster Status
    const statusEl = document.getElementById('cluster-status-text');
    if (data.cluster.offline_nodes > 0) {
        statusEl.innerText = `${data.cluster.offline_nodes} DEGRADED / OFFLINE`;
        statusEl.className = 'font-semibold text-rose-400';
    } else {
        statusEl.innerText = 'HEALTHY';
        statusEl.className = 'font-semibold text-emerald-400';
    }

    // Storage Used
    const bytes = data.cluster.total_used_bytes;
    document.getElementById('stat-used-bytes').innerText = formatBytes(bytes);
    document.getElementById('stat-total-chunks').innerText = `${data.cluster.total_chunks} Chunks`;

    // Corrupted Chunks
    const corruptedEl = document.getElementById('stat-corrupted-chunks');
    corruptedEl.innerText = data.cluster.corrupted_chunks;

    // Repair Jobs
    if (data.last_repair && data.last_repair.repaired_count > 0) {
        document.getElementById('stat-repair-jobs').innerText = data.last_repair.repaired_count;
    }

    renderWALLogs(data.wal_log);
    renderNodesControlList();
}

// Format bytes into KB/MB
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Render Chaos Control Nodes List
function renderNodesControlList() {
    const container = document.getElementById('nodes-control-list');
    if (!container) return;

    container.innerHTML = state.nodes.map(n => {
        let badgeColor = 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
        if (n.status === 'DEGRADED') badgeColor = 'bg-amber-500/20 text-amber-400 border-amber-500/30';
        if (n.status === 'OFFLINE') badgeColor = 'bg-rose-500/20 text-rose-400 border-rose-500/30';

        return `
        <div class="flex items-center justify-between p-2.5 rounded-xl bg-slate-950/80 border border-slate-800 text-xs font-mono">
            <div class="flex items-center space-x-2">
                <span class="font-bold text-slate-200">${n.node_id}</span>
                <span class="px-2 py-0.5 rounded border text-[10px] ${badgeColor}">${n.status}</span>
            </div>
            <div class="flex items-center space-x-2">
                <button onclick="setNodeStatus('${n.node_id}', 'HEALTHY')" class="px-2 py-1 rounded bg-slate-800 hover:bg-emerald-600/30 text-emerald-400 border border-slate-700">Online</button>
                <button onclick="setNodeStatus('${n.node_id}', 'OFFLINE')" class="px-2 py-1 rounded bg-slate-800 hover:bg-rose-600/30 text-rose-400 border border-slate-700">Crash Node</button>
            </div>
        </div>
        `;
    }).join('');
}

// Set Storage Node Status
async function setNodeStatus(nodeId, status) {
    try {
        const res = await fetch(`/api/cluster/nodes/${nodeId}/status`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ status: status, latency_ms: 0 })
        });
        if (res.ok) {
            fetchNodes();
            fetchObjects();
            spawnPulseRay(nodeId);
        }
    } catch (e) {
        console.error('Failed to update node status', e);
    }
}

// Populate Object & Node Select Dropdowns
function populateChaosSelects() {
    const objSelect = document.getElementById('inject-object-select');
    const nodeSelect = document.getElementById('inject-node-select');

    if (objSelect) {
        objSelect.innerHTML = '<option value="">Select Object...</option>' +
            state.objects.map(o => `<option value="${o.object_key}">${o.object_key}</option>`).join('');
    }

    if (nodeSelect) {
        nodeSelect.innerHTML = '<option value="">Select Node...</option>' +
            state.nodes.map(n => `<option value="${n.node_id}">${n.node_id}</option>`).join('');
    }
}

// Inject Bit Rot Data Corruption
async function injectCorruption() {
    const objectKey = document.getElementById('inject-object-select').value;
    const nodeId = document.getElementById('inject-node-select').value;

    if (!objectKey || !nodeId) {
        alert('Please select both an object and a storage node to corrupt.');
        return;
    }

    try {
        const res = await fetch('/api/cluster/inject-fault', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ object_key: objectKey, chunk_index: 0, node_id: nodeId })
        });
        if (res.ok) {
            alert(`Bit-Rot injected into ${objectKey} on ${nodeId}! Click 'Auto-Scrub & Repair' to see self-healing in action.`);
            fetchObjects();
            fetchNodes();
            spawnPulseRay(nodeId, '#ef4444');
        } else {
            const err = await res.json();
            alert(`Error: ${err.detail}`);
        }
    } catch (e) {
        console.error('Failed to inject corruption', e);
    }
}

// Trigger Auto Scrub & Repair Cycle
async function triggerScrub() {
    const btn = document.getElementById('btn-scrub');
    btn.disabled = true;
    btn.classList.add('opacity-50');

    try {
        const res = await fetch('/api/cluster/scrub', { method: 'POST' });
        if (res.ok) {
            const data = await res.json();
            const report = data.report;
            alert(`Integrity Scrub Complete!\nScanned Objects: ${report.objects_scanned}\nRepaired Chunks: ${report.repaired_count}`);
            fetchObjects();
            fetchNodes();
        }
    } catch (e) {
        console.error('Scrub failed', e);
    } finally {
        btn.disabled = false;
        btn.classList.remove('opacity-50');
    }
}

// Handle Object File Upload
async function handleUpload(e) {
    e.preventDefault();
    const fileInput = document.getElementById('file-input');
    const policySelect = document.getElementById('policy-select');

    if (!fileInput.files || fileInput.files.length === 0) return;

    const file = fileInput.files[0];
    const formData = new FormData();
    formData.append('object_key', file.name);
    formData.append('durability_policy', policySelect.value);
    formData.append('file', file);

    try {
        const res = await fetch('/api/objects', {
            method: 'POST',
            body: formData
        });

        if (res.ok) {
            fileInput.value = '';
            fetchObjects();
            fetchNodes();
            spawnPulseRay('node-1', '#10b981');
        } else {
            const err = await res.json();
            alert(`Upload failed: ${err.detail}`);
        }
    } catch (err) {
        console.error('Upload error', err);
    }
}

// Delete Stored Object
async function deleteObject(key) {
    if (!confirm(`Delete object '${key}' from Vault cluster?`)) return;

    try {
        const res = await fetch(`/api/objects/${encodeURIComponent(key)}`, { method: 'DELETE' });
        if (res.ok) {
            fetchObjects();
            fetchNodes();
        }
    } catch (e) {
        console.error('Failed to delete object', e);
    }
}

// Render Objects Table
function renderObjectsTable() {
    const tbody = document.getElementById('objects-table-body');
    if (!tbody) return;

    if (state.objects.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" class="p-6 text-center text-slate-500">No objects stored in Vault yet. Upload a file above!</td></tr>`;
        return;
    }

    tbody.innerHTML = state.objects.map(obj => {
        const key = obj.object_key;
        const policy = obj.durability_policy === 'erasure_coding' ? 'Reed-Solomon (4+2)' : 'Replication 3x';
        const size = formatBytes(obj.size_bytes);
        const health = obj.health_summary;

        let badge = `<span class="text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded border border-emerald-500/20">Healthy (${health.healthy_chunks})</span>`;
        if (health.corrupted_chunks > 0) {
            badge += ` <span class="text-rose-400 bg-rose-500/10 px-2 py-0.5 rounded border border-rose-500/20">Corrupted (${health.corrupted_chunks})</span>`;
        }
        if (health.missing_chunks > 0) {
            badge += ` <span class="text-amber-400 bg-amber-500/10 px-2 py-0.5 rounded border border-amber-500/20">Missing (${health.missing_chunks})</span>`;
        }

        return `
        <tr class="hover:bg-slate-900/60 transition">
            <td class="p-3 font-semibold text-slate-200">${key}</td>
            <td class="p-3 text-cyan-400">${policy}</td>
            <td class="p-3 text-slate-400">${size}</td>
            <td class="p-3">${badge}</td>
            <td class="p-3 text-right space-x-2">
                <a href="/api/objects/${encodeURIComponent(key)}" download class="px-2.5 py-1 rounded bg-slate-800 hover:bg-cyan-600/30 text-cyan-400 border border-slate-700">Download</a>
                <button onclick="deleteObject('${key}')" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-rose-600/30 text-rose-400 border border-slate-700">Delete</button>
            </td>
        </tr>
        `;
    }).join('');
}

// Render WAL Log Entries
function renderWALLogs(walEntries) {
    const container = document.getElementById('wal-log-container');
    if (!container || !walEntries) return;

    container.innerHTML = walEntries.map(e => {
        const timeStr = new Date(e.timestamp * 1000).toLocaleTimeString();
        let color = 'text-cyan-400';
        if (e.action.includes('PREPARE')) color = 'text-amber-400';
        if (e.action.includes('DELETE')) color = 'text-rose-400';

        return `
        <div class="flex items-start space-x-2">
            <span class="text-slate-600">[${timeStr}]</span>
            <span class="font-bold ${color}">${e.action}</span>
            <span class="text-slate-300">${e.key || ''}</span>
            <span class="text-slate-600 text-[10px]">tx:${e.tx_id || ''}</span>
        </div>
        `;
    }).join('');

    container.scrollTop = container.scrollHeight;
}

// Add New Node Prompt
async function addNewNodePrompt() {
    const count = state.nodes.length + 1;
    const nodeId = prompt('Enter new node ID:', `node-${count}`);
    if (!nodeId) return;

    try {
        const res = await fetch('/api/cluster/nodes', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId })
        });
        if (res.ok) {
            fetchNodes();
        } else {
            const err = await res.json();
            alert(`Error: ${err.detail}`);
        }
    } catch (e) {
        console.error('Failed to add node', e);
    }
}

// Spawn Data Ray FX on Canvas
function spawnPulseRay(nodeId, color = '#06b6d4') {
    dataRays.push({
        nodeId: nodeId,
        progress: 0,
        color: color
    });
}

// Canvas Topology Ring Render Loop
function startCanvasLoop() {
    function render() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        const centerX = canvas.width / 2;
        const centerY = canvas.height / 2;
        const radius = 120;

        // 1. Draw outer Hash Ring Circle
        ctx.beginPath();
        ctx.arc(centerX, centerY, radius, 0, 2 * Math.PI);
        ctx.strokeStyle = 'rgba(51, 65, 85, 0.6)';
        ctx.lineWidth = 3;
        ctx.stroke();

        // 2. Draw 60 Virtual Ring Ticks
        for (let i = 0; i < 60; i++) {
            const angle = (i / 60) * 2 * Math.PI;
            const x1 = centerX + (radius - 5) * Math.cos(angle);
            const y1 = centerY + (radius - 5) * Math.sin(angle);
            const x2 = centerX + (radius + 5) * Math.cos(angle);
            const y2 = centerY + (radius + 5) * Math.sin(angle);

            ctx.beginPath();
            ctx.moveTo(x1, y1);
            ctx.lineTo(x2, y2);
            ctx.strokeStyle = 'rgba(6, 182, 212, 0.2)';
            ctx.lineWidth = 1;
            ctx.stroke();
        }

        // 3. Draw Physical Storage Nodes around Ring
        const nodesCount = state.nodes.length;
        if (nodesCount > 0) {
            state.nodes.forEach((n, idx) => {
                const angle = (idx / nodesCount) * 2 * Math.PI - Math.PI / 2;
                const nodeX = centerX + (radius + 40) * Math.cos(angle);
                const nodeY = centerY + (radius + 40) * Math.sin(angle);

                // Draw connector line from ring to node
                const innerX = centerX + radius * Math.cos(angle);
                const innerY = centerY + radius * Math.sin(angle);
                ctx.beginPath();
                ctx.moveTo(innerX, innerY);
                ctx.lineTo(nodeX, nodeY);
                ctx.strokeStyle = 'rgba(51, 65, 85, 0.8)';
                ctx.lineWidth = 1.5;
                ctx.stroke();

                // Pick node color based on health
                let fillColor = '#10b981'; // Emerald
                if (n.status === 'DEGRADED') fillColor = '#f59e0b'; // Amber
                if (n.status === 'OFFLINE') fillColor = '#ef4444'; // Crimson

                // Draw Node Circle
                ctx.beginPath();
                ctx.arc(nodeX, nodeY, 14, 0, 2 * Math.PI);
                ctx.fillStyle = fillColor;
                ctx.shadowColor = fillColor;
                ctx.shadowBlur = 12;
                ctx.fill();
                ctx.shadowBlur = 0;

                // Draw Node Label
                ctx.font = '11px JetBrains Mono, monospace';
                ctx.fillStyle = '#e2e8f0';
                ctx.textAlign = 'center';
                ctx.fillText(n.node_id, nodeX, nodeY + 28);
            });
        }

        // 4. Animate Data Rays
        for (let i = dataRays.length - 1; i >= 0; i--) {
            const ray = dataRays[i];
            ray.progress += 0.04;

            const nIdx = state.nodes.findIndex(n => n.node_id === ray.nodeId);
            if (nIdx !== -1 && nodesCount > 0) {
                const angle = (nIdx / nodesCount) * 2 * Math.PI - Math.PI / 2;
                const startX = centerX;
                const startY = centerY;
                const targetX = centerX + (radius + 40) * Math.cos(angle);
                const targetY = centerY + (radius + 40) * Math.sin(angle);

                const currentX = startX + (targetX - startX) * ray.progress;
                const currentY = startY + (targetY - startY) * ray.progress;

                ctx.beginPath();
                ctx.arc(currentX, currentY, 5, 0, 2 * Math.PI);
                ctx.fillStyle = ray.color;
                ctx.shadowColor = ray.color;
                ctx.shadowBlur = 15;
                ctx.fill();
                ctx.shadowBlur = 0;
            }

            if (ray.progress >= 1) {
                dataRays.splice(i, 1);
            }
        }

        // Central Logo Text
        ctx.font = '700 16px Outfit, sans-serif';
        ctx.fillStyle = '#38bdf8';
        ctx.textAlign = 'center';
        ctx.fillText('VAULT RING', centerX, centerY + 5);

        animationFrameId = requestAnimationFrame(render);
    }

    render();
}
