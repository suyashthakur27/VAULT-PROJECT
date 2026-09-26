import hashlib
import bisect
from typing import List, Dict, Set

class ConsistentHashRing:
    """
    Consistent Hash Ring with Virtual Nodes to ensure uniform data distribution
    and minimal chunk movement during node additions/removals.
    """
    def __init__(self, vnodes_per_node: int = 100):
        self.vnodes_per_node = vnodes_per_node
        self.ring: List[int] = []  # Sorted list of hash values
        self.vnode_map: Dict[int, str] = {}  # Hash -> physical_node_id
        self.physical_nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """MD5-based 32-bit hash function mapping string key to integer ring position."""
        digest = hashlib.md5(key.encode('utf-8')).hexdigest()
        return int(digest[:8], 16)

    def add_node(self, node_id: str) -> None:
        """Add a physical storage node with multiple virtual nodes to the ring."""
        if node_id in self.physical_nodes:
            return
        
        self.physical_nodes.add(node_id)
        for i in range(self.vnodes_per_node):
            vnode_key = f"{node_id}#vnode-{i}"
            h = self._hash(vnode_key)
            bisect.insort(self.ring, h)
            self.vnode_map[h] = node_id

    def remove_node(self, node_id: str) -> None:
        """Remove a physical storage node and its virtual nodes from the ring."""
        if node_id not in self.physical_nodes:
            return
        
        self.physical_nodes.remove(node_id)
        for i in range(self.vnodes_per_node):
            vnode_key = f"{node_id}#vnode-{i}"
            h = self._hash(vnode_key)
            idx = bisect.bisect_left(self.ring, h)
            if idx < len(self.ring) and self.ring[idx] == h:
                del self.ring[idx]
                del self.vnode_map[h]

    def get_nodes(self, key: str, count: int) -> List[str]:
        """
        Find the 'count' primary physical storage nodes responsible for a given key
        by traversing clockwise along the hash ring.
        """
        if not self.ring or not self.physical_nodes:
            return []

        if count > len(self.physical_nodes):
            count = len(self.physical_nodes)

        key_hash = self._hash(key)
        idx = bisect.bisect_right(self.ring, key_hash)
        if idx == len(self.ring):
            idx = 0

        target_nodes: List[str] = []
        seen: Set[str] = set()
        initial_idx = idx

        while len(target_nodes) < count:
            h = self.ring[idx]
            node_id = self.vnode_map[h]
            if node_id not in seen:
                seen.add(node_id)
                target_nodes.append(node_id)

            idx = (idx + 1) % len(self.ring)
            if idx == initial_idx and len(target_nodes) < count:
                break  # Exhausted all available physical nodes

        return target_nodes

    def get_topology(self) -> Dict:
        """Return ring stats for visualizer."""
        return {
            "total_physical_nodes": len(self.physical_nodes),
            "total_vnodes": len(self.ring),
            "physical_nodes": sorted(list(self.physical_nodes))
        }
