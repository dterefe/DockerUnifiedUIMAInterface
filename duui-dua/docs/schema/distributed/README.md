# Distributed DUA Schemas

The `.dua` archive is not only a local file. It is the packaging format for
distributable universe, partition, and shard bundles.

## Files

- `universe-distribution-v1.schema.json`: root manifest for a distributed DUA
  universe.
- `partition-manifest-v1.schema.json`: declares a partition keyspace strategy
  and its shards.
- `shard-manifest-v1.schema.json`: declares shard objects, replicas, WAL,
  snapshots, and Merkle root.
- `routing-table-v1.schema.json`: maps partition ranges to primary/replica/cache
  URIs.
- `wal-segment-v1.md`: binary append-only replication segment.
- `archive-layout-v1.md`: complete-bundle and shard-bundle archive layouts.

## Routing Path

CAS reads must route without graph materialization:

```text
universeId
  -> distributed universe manifest
  -> partition manifest
  -> routing table
  -> shard manifest
  -> local backend: SQLite, KV, mmap/columnar, or service
```

The first executable routing implementation is
`org.texttechnologylab.duui.dua.distributed.DUADistributionPlanner`. It builds
range shards, primary/follower replica placement, `DUAShardManifest` records,
and a `DUARoutingTable` that resolves corpus/document/FS ordinals without graph
materialization.

## Bundle Modes

Complete bundle:

```text
one .dua containing all manifests, shards, objects, indexes, and WAL segments
```

Shard bundle:

```text
one .dua containing exactly one shard plus enough metadata to rejoin the universe
```

Remote bundle:

```text
manifest-only .dua with object URIs pointing to S3/MinIO/HTTP/gRPC nodes
```
