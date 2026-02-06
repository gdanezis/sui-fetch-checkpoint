#!/usr/bin/env python3
"""Fully decentralized checkpoint fetcher for Walrus-archived Sui checkpoints.

This script fetches checkpoint BCS data without relying on the walrus-sui-archival
caching server. It reads on-chain metadata from Sui, downloads the parquet index
from Walrus, finds the blob containing the target checkpoint, downloads that blob,
and extracts the checkpoint BCS data.

Centralized service dependencies:
  This script depends on two centralized services, both of which can be replaced
  by editing the constants below (SUI_RPC_URL and AGGREGATOR_BASE_URL).

  1. Sui RPC (SUI_RPC_URL) - Used to read the on-chain MetadataBlobPointer object.
     The default (fullnode.mainnet.sui.io) is a rate-limited public endpoint run by
     Mysten Labs. Alternatives include any Sui RPC provider (e.g. 1rpc.io/sui,
     sui-rpc.publicnode.com, sui.blockpi.network/v1/rpc/public) or your own fullnode.

  2. Walrus Aggregator (AGGREGATOR_BASE_URL) - Used to download blobs. The default
     (aggregator.walrus-mainnet.walrus.space) is run by the Walrus Foundation.
     Aggregators are stateless HTTP proxies over the decentralized storage node
     network -- anyone can run one with `walrus aggregator`, and community-operated
     public aggregators exist.

  The underlying data is fully decentralized: Sui state is replicated across all
  validators, and Walrus blobs are stored across 100+ independent storage nodes.

Usage: python3 fetch_checkpoint.py <checkpoint_number> [output_file]

Dependencies: pyarrow (pip install pyarrow)
"""

import base64
import json
import os
import struct
import sys
import tempfile
import urllib.request
import zlib

import pyarrow.parquet as pq

# --- Constants ---

SUI_RPC_URL = "https://fullnode.mainnet.sui.io:443"
METADATA_POINTER_OBJECT_ID = (
    "0xcdecb54e0e10e38cb2f6f69c7fdb05a5a899db3f77ff9de959b403b39f13e1c0"
)
AGGREGATOR_BASE_URL = "https://aggregator.walrus-mainnet.walrus.space/v1/blobs"
CACHE_DIR = os.path.expanduser("~/.cache/walrus-archival")

# Blob-bundle format constants
HEADER_MAGIC = b"WLBD"
FOOTER_MAGIC = b"DBLW"
FOOTER_SIZE = 4 + 4 + 8 + 4 + 4  # magic + version + index_offset + entry_count + crc32


# --- Helpers ---


def uleb128_decode(data, offset):
    """Decode a ULEB128 value from data at the given offset.
    Returns (value, new_offset)."""
    result = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, offset


def base64url_encode_nopad(data):
    """Base64url encode bytes without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sui_rpc(method, params):
    """Make a JSON-RPC call to the Sui fullnode."""
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    req = urllib.request.Request(
        SUI_RPC_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    if "error" in result:
        raise RuntimeError(f"Sui RPC error: {result['error']}")
    return result["result"]


def download_blob(blob_id, cache=False):
    """Download a full blob from the Walrus aggregator. Returns bytes.
    If cache=True, cache the blob to disk and serve from cache on subsequent calls."""
    if cache:
        cache_path = os.path.join(CACHE_DIR, "blobs", blob_id)
        if os.path.exists(cache_path):
            print(f"  Using cached blob: {cache_path}")
            with open(cache_path, "rb") as f:
                return f.read()

    url = f"{AGGREGATOR_BASE_URL}/{blob_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "walrus-sui-archival"})
    with urllib.request.urlopen(req) as resp:
        data = resp.read()

    if cache:
        os.makedirs(os.path.join(CACHE_DIR, "blobs"), exist_ok=True)
        with open(cache_path, "wb") as f:
            f.write(data)

    return data


# --- Step 1: Get metadata blob ID from Sui object ---


def get_metadata_blob_id():
    """Read the MetadataBlobPointer object from Sui and return the blob ID string."""
    result = sui_rpc(
        "sui_getObject",
        [METADATA_POINTER_OBJECT_ID, {"showBcs": True}],
    )

    bcs_data = result["data"]["bcs"]
    if bcs_data["dataType"] != "moveObject":
        raise RuntimeError("Expected moveObject")

    raw_bcs = base64.b64decode(bcs_data["bcsBytes"])

    # BCS layout: MetadataBlobPointer { id: UID(32 bytes), blob_id: Option<Vec<u8>> }
    offset = 32  # skip UID

    # Read Option tag
    option_tag = raw_bcs[offset]
    offset += 1

    if option_tag == 0:
        raise RuntimeError("MetadataBlobPointer.blob_id is None")
    elif option_tag != 1:
        raise RuntimeError(f"Unexpected Option tag: {option_tag}")

    # Read Vec<u8> length (ULEB128) then bytes
    vec_len, offset = uleb128_decode(raw_bcs, offset)
    if vec_len != 32:
        raise RuntimeError(f"Expected 32-byte blob_id, got {vec_len}")

    blob_id_bytes = raw_bcs[offset : offset + 32]
    return base64url_encode_nopad(blob_id_bytes)


# --- Step 2 & 3: Cache and parquet parsing ---


def get_cache_path(metadata_blob_id):
    """Return the cache file path for a metadata blob ID."""
    return os.path.join(CACHE_DIR, f"metadata_{metadata_blob_id}.json")


def decode_varint(data, pos):
    """Decode a protobuf varint. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise RuntimeError("Unexpected end of data while reading varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, pos


def decode_protobuf_checkpoint_blob_info(data):
    """Decode a CheckpointBlobInfo protobuf message.
    Returns dict with blob_id (str), start_checkpoint (int), end_checkpoint (int).
    """
    pos = 0
    blob_id = None
    start_checkpoint = 0
    end_checkpoint = 0

    while pos < len(data):
        tag, pos = decode_varint(data, pos)
        field_number = tag >> 3
        wire_type = tag & 0x07

        if wire_type == 0:  # varint
            value, pos = decode_varint(data, pos)
            if field_number == 4:
                start_checkpoint = value
            elif field_number == 5:
                end_checkpoint = value
        elif wire_type == 2:  # length-delimited
            length, pos = decode_varint(data, pos)
            value = data[pos : pos + length]
            pos += length
            if field_number == 2:
                # blob_id is stored as UTF-8 bytes of the base64url string
                blob_id = value.decode("utf-8")
        else:
            raise RuntimeError(f"Unsupported wire type {wire_type} for field {field_number}")

    return {
        "blob_id": blob_id,
        "start_checkpoint": start_checkpoint,
        "end_checkpoint": end_checkpoint,
    }


def load_or_fetch_metadata_index(metadata_blob_id):
    """Load the metadata index from cache or download+parse the parquet blob."""
    cache_path = get_cache_path(metadata_blob_id)

    if os.path.exists(cache_path):
        print(f"  Using cached metadata index: {cache_path}")
        with open(cache_path) as f:
            return json.load(f)

    print(f"  Downloading metadata parquet blob {metadata_blob_id}...")
    parquet_data = download_blob(metadata_blob_id)

    # Write to temp file for pyarrow to read
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(parquet_data)
        tmp_path = tmp.name

    try:
        pf = pq.ParquetFile(tmp_path)
        entries = []
        for batch in pf.iter_batches():
            col = batch.column("checkpoint_blob_info")
            for val in col:
                raw = val.as_py()  # bytes
                info = decode_protobuf_checkpoint_blob_info(raw)
                entries.append(info)
    finally:
        os.unlink(tmp_path)

    # Save to cache
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(entries, f)
    print(f"  Cached {len(entries)} blob entries to {cache_path}")

    return entries


# --- Step 4: Find target checkpoint's blob ---


def find_checkpoint_blob(entries, checkpoint):
    """Find the entry whose range contains the target checkpoint."""
    for entry in entries:
        if entry["start_checkpoint"] <= checkpoint <= entry["end_checkpoint"]:
            return entry
    return None


# --- Step 5: Download blob and parse blob-bundle index ---


def parse_blob_bundle_footer(data):
    """Parse the blob-bundle footer from the last 24 bytes.
    Returns (index_offset, entry_count)."""
    if len(data) < FOOTER_SIZE:
        raise RuntimeError("Blob too small to contain footer")

    footer = data[-FOOTER_SIZE:]
    magic = footer[0:4]
    if magic != FOOTER_MAGIC:
        raise RuntimeError(f"Invalid footer magic: {magic!r}")

    version = struct.unpack_from("<I", footer, 4)[0]
    if version != 1:
        raise RuntimeError(f"Unsupported blob-bundle version: {version}")

    index_offset = struct.unpack_from("<Q", footer, 8)[0]
    entry_count = struct.unpack_from("<I", footer, 16)[0]
    footer_crc = struct.unpack_from("<I", footer, 20)[0]

    # Validate footer CRC32 (covers magic + version + index_offset + entry_count)
    calculated_crc = zlib.crc32(footer[0:20]) & 0xFFFFFFFF
    if calculated_crc != footer_crc:
        raise RuntimeError(
            f"Footer CRC32 mismatch: expected {footer_crc:#010x}, got {calculated_crc:#010x}"
        )

    return index_offset, entry_count


def parse_blob_bundle_index(data, index_offset, entry_count):
    """Parse blob-bundle index entries.
    Returns list of (id_str, offset, length)."""
    pos = index_offset
    entries = []

    for _ in range(entry_count):
        id_len = struct.unpack_from("<I", data, pos)[0]
        pos += 4
        entry_id = data[pos : pos + id_len].decode("utf-8")
        pos += id_len
        offset = struct.unpack_from("<Q", data, pos)[0]
        pos += 8
        length = struct.unpack_from("<Q", data, pos)[0]
        pos += 8
        _crc32 = struct.unpack_from("<I", data, pos)[0]
        pos += 4
        entries.append((entry_id, offset, length))

    return entries


def extract_checkpoint_from_blob(blob_data, checkpoint):
    """Download a blob-bundle and extract the checkpoint data."""
    index_offset, entry_count = parse_blob_bundle_footer(blob_data)
    index_entries = parse_blob_bundle_index(blob_data, index_offset, entry_count)

    target_id = str(checkpoint)
    for entry_id, offset, length in index_entries:
        if entry_id == target_id:
            return blob_data[offset : offset + length]

    raise RuntimeError(
        f"Checkpoint {checkpoint} not found in blob-bundle index. "
        f"Available: {[e[0] for e in index_entries[:5]]}..."
    )


# --- Main ---


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <checkpoint_number> [output_file]", file=sys.stderr)
        sys.exit(1)

    checkpoint = int(sys.argv[1])
    output_file = sys.argv[2] if len(sys.argv) > 2 else f"checkpoint_{checkpoint}.bcs"

    print(f"Fetching checkpoint {checkpoint}...")

    # Step 1: Get metadata blob ID from Sui
    print("Step 1: Reading metadata pointer from Sui...")
    metadata_blob_id = get_metadata_blob_id()
    print(f"  Metadata blob ID: {metadata_blob_id}")

    # Step 2+3: Load or download metadata index
    print("Step 2: Loading metadata index...")
    entries = load_or_fetch_metadata_index(metadata_blob_id)
    print(f"  Index contains {len(entries)} blob entries")

    # Step 4: Find which blob contains our checkpoint
    print(f"Step 3: Finding blob for checkpoint {checkpoint}...")
    blob_entry = find_checkpoint_blob(entries, checkpoint)
    if blob_entry is None:
        print(
            f"Error: Checkpoint {checkpoint} not found in any blob",
            file=sys.stderr,
        )
        if entries:
            print(
                f"  Available range: {entries[0]['start_checkpoint']} - "
                f"{entries[-1]['end_checkpoint']}",
                file=sys.stderr,
            )
        sys.exit(1)

    print(
        f"  Found in blob {blob_entry['blob_id']} "
        f"(checkpoints {blob_entry['start_checkpoint']}-{blob_entry['end_checkpoint']})"
    )

    # Step 5: Download blob and extract checkpoint
    print(f"Step 4: Downloading blob {blob_entry['blob_id']}...")
    blob_data = download_blob(blob_entry["blob_id"], cache=True)
    print(f"  Blob size: {len(blob_data)} bytes")

    print(f"Step 5: Extracting checkpoint {checkpoint}...")
    checkpoint_bcs = extract_checkpoint_from_blob(blob_data, checkpoint)

    # Step 6: Save
    with open(output_file, "wb") as f:
        f.write(checkpoint_bcs)

    print(f"Saved to {output_file} ({len(checkpoint_bcs)} bytes)")


if __name__ == "__main__":
    main()
