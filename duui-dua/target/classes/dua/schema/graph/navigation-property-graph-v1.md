# DUA Navigation Property Graph Schema v1

Target engines: PostgreSQL AGE, Neo4j-style property graph, JSONL graph codec,
SQLite graph codec.

This graph is not the CAS payload store. It is a navigation projection over
feature-structure ids, payload ids, documents, views, and generated indexes.

## Node Labels

### `Universe`

Properties:

- `id: string`
- `formatVersion: string`

### `Corpus`

Properties:

- `id: string`
- `name: string`

### `Document`

Properties:

- `id: string`
- `externalId: string`

### `View`

Properties:

- `id: string`
- `name: string`
- `scope: string`

### `FeatureStructure`

Properties:

- `id: string`
- `fsRef: integer`
- `typeCode: integer`
- `typeName: string`
- `viewId: integer`

### `Payload`

Properties:

- `id: string`
- `mediaType: string`
- `path: string`

### `IndexEntry`

Properties:

- `id: string`
- `indexName: string`
- `key: string`

## Edge Labels

| edge | source | target | properties |
| --- | --- | --- | --- |
| `CONTAINS` | `Universe/Corpus/Document/View` | any child | `order` |
| `HAS_VIEW` | `Document/Corpus` | `View` | none |
| `HAS_SOFA` | `View` | `Payload` | `mimeType` |
| `MATERIALIZES` | `FeatureStructure` | `Payload` | `role` |
| `FEATURE_REF` | `FeatureStructure` | `FeatureStructure` | `featureName` |
| `INDEXES` | `IndexEntry` | `FeatureStructure` | `score`, `offset` |
| `EQUIVALENT_TO` | any | any | `basis` |
| `MEMBER_OF` | any | any | `order` |
| `REFERENCES` | any | any | `role` |
| `NEXT` | any | any | `order` |

## AGE DDL Sketch

```sql
SELECT create_graph('dua_nav');

-- AGE stores labels dynamically. These indexes are the required operational
-- indexes when mirrored into relational side tables:
CREATE TABLE IF NOT EXISTS dua_nav_nodes (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    properties JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS dua_nav_edges (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    properties JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dua_nav_nodes_label ON dua_nav_nodes(label);
CREATE INDEX IF NOT EXISTS idx_dua_nav_edges_source_label ON dua_nav_edges(source_id, label);
CREATE INDEX IF NOT EXISTS idx_dua_nav_edges_target_label ON dua_nav_edges(target_id, label);
```
