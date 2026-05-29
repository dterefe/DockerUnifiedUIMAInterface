# DUA Distributed WAL Segment v1

Target: append-only change replication between DUA shards.

Binary record framing:

```text
magic:u32 = 0x44574131  -- "DWA1"
segment_header_length:u32
segment_header:json
record*
```

Record:

```text
record_length:u32
sequence:u64
epoch:u64
operation:u8
key_length:u16
key:bytes
payload_length:u32
payload:bytes
crc32c:u32
```

Operation codes:

| code | operation |
| --- | --- |
| `1` | put slot |
| `2` | delete slot |
| `3` | initialize array |
| `4` | put array element |
| `5` | delete array element |
| `6` | put payload object |
| `7` | delete payload object |
| `8` | graph edge/node projection update |
| `9` | checkpoint |

Segment header JSON:

```json
{
  "schema": "dua.distributed.wal.segment.v1",
  "universeId": "u",
  "partitionId": "p",
  "shardId": "s",
  "firstSequence": 1,
  "lastSequence": 1000,
  "previousSegmentSha256": null
}
```

The WAL key uses the same physical key encoding as the target partition. For CAS
KV partitions this is `cas-keyspace-v1`; for object partitions it is the object
key path.
