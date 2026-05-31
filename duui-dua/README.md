# DUA - DUUI Universe of Artifacts

DUA is the current DUUI module for a UIMA-compatible artifact universe:

- `DUA` is the artifact universe handle over an installed UIMA view.
- `JDUA<T>` is the typed projection handle for explicit corpus/document/domain modes.
- `DUABackend` is the CASImpl-level storage and semantic query substrate.
- `DUABackendLayout` describes backend stores by semantic role instead of by
  implementation technology.
- `.dua` archives are persistence and transfer containers, not the runtime API.
- DUUI document clients read, write, list, and explore environment objects.
- Shared DUUI document readers and writers own deserialization, serialization,
  metadata, caching, prefetching, and source/target integration for Composer and
  V2 Orchestrator.
- XMI/CAS import remaps source feature-structure ids into DUA-global ids.

## Kept Runtime Pieces

- Patched UIMA backend hook classes under `org.apache.uima...`.
- CAS storage implementations under `org.texttechnologylab.duui.dua.uima`.
- Backend layout descriptors for relational value, annotation range, type graph,
  text search, and archive payload stores.
- Minimal `.dua` archive reader/writer for persistence and transfer payloads,
  type systems, CAS payloads, and indexes.
- XMI payload bridge for UIMA serialization and materialization.
- Minimal semantic query contracts for annotation span lookup.

## Backend Semantics

The backend surface is split into semantic stores:

- `RELATIONAL_VALUE`: CAS feature values, primitive arrays, reference arrays,
  FS collections, and global FS-id allocation.
- `ANNOTATION_RANGE`: annotation spans, containment, overlap, range joins,
  same-span joins, neighborhoods, and covered-text lookup.
- `TYPESYSTEM_GRAPH`: type hierarchy queries and feature-reference traversal.
- `TEXT_SEARCH`: sofa text, covered text, exact text, and substring lookup.
- `ARCHIVE_PAYLOAD`: archive bytes for persisted payload families.

The current in-process backend maps the relational value role onto
`DUACasStorage`. The intended PostgreSQL deployment maps these roles onto
separate tables/indexing strategies: relational feature tables, GiST-backed
range indexes, type graph tables/extensions, and trigram/text-search indexes.

## Archive Layout

```text
dua.json
artifacts/
typesystems/
cas/
indexes/
```
