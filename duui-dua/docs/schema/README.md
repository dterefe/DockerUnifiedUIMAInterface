# DUA Storage Schemas

DUA has one storage model with several versioned physical contracts. The
contracts are not competing storage models:

- canonical CAS state is addressed by identifier-first slot/array/string keys;
- wide corpus queries use typed projections;
- graph data is a navigation projection;
- `.dua` archives package manifests, shard objects, payloads, and schemas.

The versioned schema/contract files live under:

`src/main/resources/dua/schema/`

## Implemented Schema Contracts

| contract | schema file | purpose |
| --- | --- | --- |
| Relational / SQLite | `sqlite/cas-storage-v1.sql` | mutable identifier-first CAS slot and array storage |
| Key-value / LSM | `kv/cas-keyspace-v1.md` | point-read/write CAS storage for RocksDB/FoundationDB/LMDB-style engines |
| Document store | `document/cas-document-v1.schema.json` | MongoDB/Couchbase/OpenSearch-style FS document records |
| Columnar / mmap / Parquet | `columnar/cas-columnar-v1.md` | read-mostly analytical and inspector scans |
| Property graph | `graph/navigation-property-graph-v1.md` | navigation projection, not hot CAS payload storage |
| Object/blob | `object/payload-layout-v1.md` | stream-first payload storage for text, XMI, media, embeddings |
| PostgreSQL query projection | `postgres/query-backend-v1.sql` | typed corpus-wide fulltext, metadata, SRL, geo/time, association, and inspector query backend |
| PostgreSQL query patterns | `postgres/query-patterns-v1.sql` | candidate-set query primitives over the typed projection schema |
| Document transfer package | `transport/document-transfer-v1.schema.json` | single/multi-document transport manifest for XMI and native DUA payload import/export |
| Service module descriptor | `service/module-descriptor-v1.schema.json` | pluggable DUA module/service ownership, protocol, interaction, and performance contract |
| Service API | `service/storage-service-v1.openapi.yaml` | microservice contract for remote CAS storage operations |
| gRPC Service API | `service/grpc/storage-service-v1.proto` | binary service contract for low-overhead remote CAS operations |
| gRPC Transport API | `service/grpc/transport-service-v1.proto` | binary service contract for XMI and `.dua-transfer` import/export jobs |
| Distributed universe | `distributed/universe-distribution-v1.schema.json` | top-level distributed universe manifest |
| Distributed partition | `distributed/partition-manifest-v1.schema.json` | partition keyspace, shard, and index manifest |
| Distributed shard | `distributed/shard-manifest-v1.schema.json` | shard object, replica, WAL, snapshot, and Merkle metadata |
| Routing table | `distributed/routing-table-v1.schema.json` | partition/shard route resolution |
| WAL segment | `distributed/wal-segment-v1.md` | append-only binary change segment layout |
| Distributed archive layout | `distributed/archive-layout-v1.md` | complete-bundle and shard-bundle `.dua` layouts |

## Hot Path Rule

CAS-compatible `JCas` reads must resolve by direct identity:

```text
fs_ref + feature
fs_ref + array_kind + index
string_code
```

Graph traversal is never required for ordinary CAS slot/array reads.

## Wide Query Rule

Corpus-wide search and inspector queries must use typed projection schemas such
as `postgres/query-backend-v1.sql`. Graph stores are navigation projections; hot
query predicates must not depend on extracting and casting graph properties.

## Correctness Rule

Every backend must first match ordinary heap JCas behavior for:

- default primitive values,
- null string/reference values,
- array length and bounds,
- sparse array defaults,
- FS id lookup through UIMA's id-to-FS map,
- concurrent writes to independent FS/feature keys.

Only after that should we compare read latency, memory plateau, and virtual
thread concurrency thresholds.
