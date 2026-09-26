import hashlib
from typing import List, Tuple, Dict, Optional

# ==========================================
# GF(2^8) Galois Field Arithmetic Engine
# ==========================================
PRIM = 0x11d  # x^8 + x^4 + x^3 + x^2 + 1

GF_EXP = [0] * 512
GF_LOG = [0] * 256

def _init_gf_tables():
    x = 1
    for i in range(255):
        GF_EXP[i] = x
        GF_LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= PRIM
    for i in range(255, 512):
        GF_EXP[i] = GF_EXP[i - 255]

_init_gf_tables()

def gf_add(x: int, y: int) -> int:
    return x ^ y

def gf_sub(x: int, y: int) -> int:
    return x ^ y  # In GF(2^8), addition and subtraction are XOR

def gf_mul(x: int, y: int) -> int:
    if x == 0 or y == 0:
        return 0
    return GF_EXP[GF_LOG[x] + GF_LOG[y]]

def gf_div(x: int, y: int) -> int:
    if y == 0:
        raise ZeroDivisionError("GF(2^8) Division by zero")
    if x == 0:
        return 0
    return GF_EXP[(GF_LOG[x] + 255 - GF_LOG[y]) % 255]

def gf_inv(x: int) -> int:
    if x == 0:
        raise ZeroDivisionError("GF(2^8) Inverse of zero")
    return GF_EXP[255 - GF_LOG[x]]


# Matrix Operations over GF(2^8)
def invert_matrix_gf(matrix: List[List[int]]) -> List[List[int]]:
    """Invert a square matrix over GF(2^8) using Gauss-Jordan elimination."""
    n = len(matrix)
    # Augment with identity matrix
    aug = [row[:] + [1 if i == j else 0 for j in range(n)] for i, row in enumerate(matrix)]

    for i in range(n):
        # Find pivot
        pivot = aug[i][i]
        if pivot == 0:
            for k in range(i + 1, n):
                if aug[k][i] != 0:
                    aug[i], aug[k] = aug[k], aug[i]
                    pivot = aug[i][i]
                    break
        if pivot == 0:
            raise ValueError("Matrix is singular over GF(2^8)")

        # Scale pivot row to 1
        inv_pivot = gf_inv(pivot)
        for j in range(2 * n):
            aug[i][j] = gf_mul(aug[i][j], inv_pivot)

        # Eliminate column entries in other rows
        for k in range(n):
            if k != i:
                factor = aug[k][i]
                if factor != 0:
                    for j in range(2 * n):
                        aug[k][j] = gf_sub(aug[k][j], gf_mul(factor, aug[i][j]))

    return [row[n:] for row in aug]


class ReedSolomonEC:
    """
    Systematic Reed-Solomon Erasure Coding (K data shards + M parity shards).
    Can recover original payload if any K shards out of (K+M) are available.
    """
    def __init__(self, data_shards: int = 4, parity_shards: int = 2):
        self.k = data_shards
        self.m = parity_shards
        self.total = data_shards + parity_shards
        self.enc_matrix = self._generate_encoding_matrix()

    def _generate_encoding_matrix(self) -> List[List[int]]:
        """Generate systematic (K+M) x K matrix [Identity_K ; Cauchy_M_x_K]."""
        matrix = [[0] * self.k for _ in range(self.total)]
        # Top K x K is Identity
        for i in range(self.k):
            matrix[i][i] = 1

        # Bottom M x K is Cauchy matrix: 1 / (X_i ^ Y_j) where X_i and Y_j are disjoint sets
        for i in range(self.m):
            x_val = self.k + i
            for j in range(self.k):
                y_val = j
                matrix[self.k + i][j] = gf_inv(gf_sub(x_val, y_val))

        return matrix

    def encode(self, payload: bytes) -> List[Tuple[int, bytes, str]]:
        """
        Splits payload into K data shards, computes M parity shards.
        Returns list of tuples: (shard_index, shard_bytes, shard_sha256_checksum).
        """
        # Ensure payload length is multiple of K
        original_len = len(payload)
        padding_len = (self.k - (original_len % self.k)) % self.k
        padded_payload = payload + b'\x00' * padding_len
        shard_size = len(padded_payload) // self.k

        # Break into K data shards
        data_shards = [
            padded_payload[i * shard_size:(i + 1) * shard_size]
            for i in range(self.k)
        ]

        shards: List[bytes] = list(data_shards)

        # Compute M parity shards byte by byte
        for i in range(self.m):
            parity_row = self.enc_matrix[self.k + i]
            parity_buf = bytearray(shard_size)
            for byte_idx in range(shard_size):
                val = 0
                for j in range(self.k):
                    coeff = parity_row[j]
                    d_byte = data_shards[j][byte_idx]
                    val = gf_add(val, gf_mul(coeff, d_byte))
                parity_buf[byte_idx] = val
            shards.append(bytes(parity_buf))

        result = []
        for idx, shard_data in enumerate(shards):
            checksum = hashlib.sha256(shard_data).hexdigest()
            result.append((idx, shard_data, checksum))

        return result

    def decode(self, available_shards: Dict[int, bytes], original_size: int) -> bytes:
        """
        Decode original payload using any K surviving shards out of (K+M).
        available_shards: dict mapping shard_index (0..K+M-1) to shard_bytes.
        """
        if len(available_shards) < self.k:
            raise ValueError(f"Insufficient shards for reconstruction: need {self.k}, got {len(available_shards)}")

        # Pick the first K available shard indices
        surviving_indices = sorted(list(available_shards.keys()))[:self.k]

        # Extract submatrix for surviving indices
        sub_matrix = [self.enc_matrix[idx] for idx in surviving_indices]

        # Invert the K x K submatrix
        inv_sub_matrix = invert_matrix_gf(sub_matrix)

        shard_size = len(next(iter(available_shards.values())))
        reconstructed_data_shards = [bytearray(shard_size) for _ in range(self.k)]

        for byte_idx in range(shard_size):
            # For each data shard j (0..K-1)
            for j in range(self.k):
                val = 0
                inv_row = inv_sub_matrix[j]
                for s_pos, s_idx in enumerate(surviving_indices):
                    coeff = inv_row[s_pos]
                    s_byte = available_shards[s_idx][byte_idx]
                    val = gf_add(val, gf_mul(coeff, s_byte))
                reconstructed_data_shards[j][byte_idx] = val

        full_padded = b"".join(bytes(s) for s in reconstructed_data_shards)
        return full_padded[:original_size]


class ReplicationEngine:
    """
    N-Way Replication Strategy with Quorum Read/Write validation.
    """
    def __init__(self, replicas: int = 3, write_quorum: int = 2, read_quorum: int = 2):
        self.n = replicas
        self.w = write_quorum
        self.r = read_quorum

    def create_replicas(self, payload: bytes) -> List[Tuple[int, bytes, str]]:
        """Create N identical replicas with sha256 checksums."""
        checksum = hashlib.sha256(payload).hexdigest()
        return [(idx, payload, checksum) for idx in range(self.n)]

    def resolve_quorum_read(self, replica_chunks: List[Tuple[bytes, str]]) -> Tuple[bytes, str]:
        """
        Perform quorum read consensus across available replica chunks.
        Discards corrupted chunks whose SHA256 doesn't match content,
        and returns the consensus majority valid payload.
        """
        valid_chunks: Dict[str, Tuple[bytes, int]] = {}  # sha256 -> (data, count)

        for data, expected_hash in replica_chunks:
            actual_hash = hashlib.sha256(data).hexdigest()
            if actual_hash == expected_hash:
                if actual_hash not in valid_chunks:
                    valid_chunks[actual_hash] = (data, 1)
                else:
                    d, cnt = valid_chunks[actual_hash]
                    valid_chunks[actual_hash] = (d, cnt + 1)

        if not valid_chunks:
            raise ValueError("All replica reads failed checksum verification!")

        # Find chunk with highest consensus
        best_hash, (best_data, count) = max(valid_chunks.items(), key=lambda x: x[1][1])
        if count < 1:
            raise ValueError("Read Quorum not met!")

        return best_data, best_hash
