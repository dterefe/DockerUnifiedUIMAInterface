# Distributed DUA Archive Layout v1

A `.dua` archive can be a complete bundle or a distributable shard bundle. The
same logical universe may therefore be represented by many archive objects.

## Complete Bundle

```text
dua.json
distribution/universe.json
distribution/routing/{routing_table_id}.json
partitions/{partition_id}/manifest.json
partitions/{partition_id}/shards/{shard_id}/manifest.json
partitions/{partition_id}/shards/{shard_id}/objects/**
partitions/{partition_id}/shards/{shard_id}/wal/**
schemas/**
```

## Shard Bundle

```text
dua-shard.json
distribution/shard.json
objects/**
wal/**
snapshot/**
indexes/**
```

## Object URI Forms

```text
dua://{universeId}/partitions/{partitionId}/shards/{shardId}/objects/{objectId}
dua+zip://{archivePath}!/partitions/{partitionId}/shards/{shardId}/objects/{objectId}
s3://{bucket}/{prefix}/partitions/{partitionId}/shards/{shardId}/objects/{objectId}
http://{node}/dua/v1/partitions/{partitionId}/shards/{shardId}/objects/{objectId}
```

## Distribution Rule

No partition may require global graph materialization for CAS reads. A CAS read
routes directly to one shard by:

```text
universeId -> partitionId -> routing table -> shardId -> local backend
```
