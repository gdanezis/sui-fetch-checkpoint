# Decentralized Checkpoint Fetcher

`fetch_checkpoint.py` fetches Sui checkpoint BCS data directly from the Sui
blockchain and Walrus decentralized storage, without relying on the
walrus-sui-archival caching server.

## How it works

1. **Read on-chain pointer** -- Queries a Sui fullnode for the
   `MetadataBlobPointer` object, which contains the Walrus blob ID of the
   current metadata parquet file.
2. **Download metadata index** -- Fetches the parquet blob from a Walrus
   aggregator and parses it to build an index of which blob contains which
   checkpoint range.
3. **Find target blob** -- Looks up the target checkpoint number in the index.
4. **Download and extract** -- Fetches the blob-bundle from Walrus, parses its
   binary index, and extracts the checkpoint BCS data.

## Usage

```
python3 fetch_checkpoint.py <checkpoint_number> [output_file]
```

Output file defaults to `checkpoint_<number>.bcs`.

### Examples

```bash
# Fetch checkpoint 1000000
python3 scripts/fetch_checkpoint.py 1000000

# Fetch with a custom output path
python3 scripts/fetch_checkpoint.py 1000000 /tmp/my_checkpoint.bcs
```

## Dependencies

- Python 3
- [pyarrow](https://pypi.org/project/pyarrow/) (`pip install pyarrow`)
- All other imports are Python standard library

## Caching

The script caches two things under `~/.cache/walrus-archival/`:

- **Metadata index** (`metadata_<blob_id>.json`) -- The parsed parquet index
  mapping checkpoint ranges to blob IDs. Cached per metadata blob ID, so a new
  parquet is downloaded automatically when the on-chain pointer updates.
- **Checkpoint blobs** (`blobs/<blob_id>`) -- The raw blob-bundle files. Since
  each blob contains a range of checkpoints, fetching a second checkpoint from
  the same blob is served entirely from cache.

To force a fresh download, delete the cache directory:

```bash
rm -rf ~/.cache/walrus-archival
```

## Centralized service dependencies

The underlying data is fully decentralized: Sui state is replicated across all
validators, and Walrus blobs are stored across 100+ independent storage nodes.
However, the script accesses this data through two centralized gateway services.
Both can be swapped by editing the constants at the top of the script.

### Sui RPC (`SUI_RPC_URL`)

Used to read the on-chain `MetadataBlobPointer` object. The default
(`fullnode.mainnet.sui.io`) is a rate-limited public endpoint run by Mysten
Labs.

Alternatives:
- `https://1rpc.io/sui`
- `https://sui-rpc.publicnode.com`
- `https://sui.blockpi.network/v1/rpc/public`
- Run your own Sui fullnode

### Walrus Aggregator (`AGGREGATOR_BASE_URL`)

Used to download blobs. The default (`aggregator.walrus-mainnet.walrus.space`)
is run by the Walrus Foundation. Aggregators are stateless HTTP proxies over
the decentralized storage node network -- anyone can run one with
`walrus aggregator`, and community-operated public aggregators exist.
