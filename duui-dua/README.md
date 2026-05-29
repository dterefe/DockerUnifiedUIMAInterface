# DUA - DUUI Universe of Artifacts

DUA is a portable `.dua` ZIP container for DUUI artifact universes. It stores
artifact payloads, graph partitions, CAS/XMI compatibility payloads, and indexes
under a stable archive layout.

## Archive Layout

```text
dua.json
artifacts/
graphs/
typesystems/
cas/
indexes/
```

`dua.json` is the manifest. It records the universe id, format version, artifact
payload entries, and graph partition entries.

## Graph Codecs

The graph SPI is centered on `DUAGraphCodec`, `DUAGraphPartition`,
`DUAGraphNode`, and `DUAGraphEdge`.

Implemented codecs:

- `jsonl`: inspectable property graph records in JSON Lines.
- `sqlite`: indexed property graph tables in an embedded SQLite database.

## UIMA CAS Backend Override

The module vendors patched UIMA 3.6 implementation classes from the
`duui-backend-3.6` fork branch under their original `org.apache.uima...`
packages. These classes shadow the corresponding `uimaj-core` classes when this
module appears first on the runtime classpath.

The backend hook is `org.apache.uima.cas.impl.Backend`. DUA provides:

- `DUACasBackendInstaller` to attach a backend to a CAS/JCas.
- `DUABackedCasFactory` to create a JCas with a backend already installed.
- `DUAMemoryBackend` as the first complete backend contract implementation for
  slot, string, array, lifecycle, and collection operations.

This removes the dependency on DUUI core. DUA is its own module and only depends
on UIMA/Jackson/SQLite.

## CAS/XMI Bridge

`DUAXmiBridge` can:

- import XMI files/directories into a DUA archive,
- store a `JCas` as an XMI artifact payload,
- create a CAS graph partition for corpus/document/view/indexed FS structure,
- materialize the stored payload back into `JCas`,
- export stored XMI payloads for existing UIMA workflows.

This keeps existing DUUI annotators on lazy `JCas` materialization while DUA
becomes the broader artifact-universe container.

## Document Transport

See `docs/document-transport-format.md` for the DUA document transfer package.
Bare XMI remains the single-document compatibility export/import format.
`.dua-transfer` is the batch format for one or many documents, corpus membership
updates, view payloads, type system references, and checksummed native DUA/XMI
payloads.

## Modular Service Architecture

See `docs/modular-service-architecture.md` for the full DUA service-module
contract. DUA Core owns canonical corpus, CAS, payload, type-system, revision,
and pipeline-window state. Fulltext, annotation analytics, metadata/ontology,
semantic event, geotemporal, vector, graph navigation, inspector, governance,
and telemetry capabilities attach as explicit pluggable services. The same
contract is available in code through
`org.texttechnologylab.duui.dua.service.DUAServiceModuleCatalog` and as an
external descriptor schema at
`src/main/resources/dua/schema/service/module-descriptor-v1.schema.json`.

## Concept Model

The first expandable concept layer lives in:

- `org.texttechnologylab.duui.dua.model`
- `org.texttechnologylab.duui.dua.store`
- `org.texttechnologylab.duui.dua.query`
- `org.texttechnologylab.duui.dua.inspect`

See `docs/concept-structure.md` for the entity/store/query/UI vocabulary. The
model keeps UCE-style Domain and Association semantics as first-class sealed
records while treating graph partitions as navigation projections over the
identifier-based feature-structure and payload stores.

## Storage Backends

See `docs/storage-model.md` for the CAS storage model. The
current testable storage layer includes:

- ordinary UIMA heap CAS as the baseline,
- `DUAConcurrentMemoryCasStorage` for concurrent in-process testing,
- `DUADenseMemoryCasStorage` for hot primitive slot reads and virtual-thread
  write stress,
- `DUAOrderedKvCasStorage` for a durable ordered key-value shard with typed
  slot, array, string dictionary, lifecycle, counter, and append-only WAL
  records,
- `DUASqliteCasStorage` for identifier-first local persistence experiments,
- `DUATieredCasStorage` for bounded dense-memory hot sets with ordered KV,
  SQLite, or another durable backend underneath.

The actual versioned schemas/contracts are indexed in `docs/schema/README.md`
and stored under `src/main/resources/dua/schema/`.

## Wide Corpus Queries

See `docs/query-backend-architecture.md` for the BioFID/UCE query projection
evaluation. DUA separates lazy CAS storage from corpus-wide query projections:
PostgreSQL typed projection tables handle fulltext, metadata, semantic-role,
geotemporal, association, and inspector queries, while graph codecs remain
navigation/export layers.

See `docs/backend-stress-tests.md` for the executable backend stress tests that
use UCE query functions as workload references.

## Distribution

The package `org.texttechnologylab.duui.dua.distributed` contains the first
executable distributed archive model: shard manifests, routing tables, replica
placement, and deterministic ordinal-to-shard lookup. It matches the distributed
schemas under `src/main/resources/dua/schema/distributed/` and gives the archive
format a concrete path to complete bundles, shard bundles, and remote
manifest-only bundles.

## UCE Runtime

See `docs/uce-runtime-service-architecture.md` for the non-portability-first UCE
deployment architecture: FoundationDB for canonical CAS keys, S3-compatible
object storage for payloads, Redpanda for events, PostgreSQL for registry/ACL,
OpenSearch for fulltext, ClickHouse for inspector analytics, and bounded DUA
CAS materialization services for JCas access.
