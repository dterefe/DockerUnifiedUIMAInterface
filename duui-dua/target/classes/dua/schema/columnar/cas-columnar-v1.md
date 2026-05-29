# DUA CAS Columnar Layout v1

Target engines: memory-mapped files, Arrow IPC, Parquet, DuckDB external tables.

This layout is read-mostly. It is meant for fast inspector queries, batch scans,
and low-memory streaming, not primary mutable CAS writes.

## Partitioning

```text
cas-columnar/v1/
  manifest.json
  fs_lifecycle.parquet
  slots/
    feature_hash=<u64>/part-*.parquet
  arrays/
    kind=<array-kind>/part-*.parquet
  strings.parquet
```

## `manifest.json`

```json
{
  "schema": "dua.cas.columnar.v1",
  "sortOrder": ["view_id", "type_code", "fs_ref"],
  "features": [
    {
      "featureName": "uima.tcas.Annotation:begin",
      "featureHash": "0000000000000001",
      "valueKind": "INTEGER",
      "path": "slots/feature_hash=0000000000000001/"
    }
  ]
}
```

## `fs_lifecycle`

| column | type |
| --- | --- |
| `fs_ref` | `uint64` |
| `type_code` | `int32` |
| `view_id` | `int32` |
| `deleted` | `bool` |

## Slot Partitions

One partition per hot feature.

| column | type |
| --- | --- |
| `view_id` | `int32` |
| `type_code` | `int32` |
| `fs_ref` | `uint64` |
| `value_boolean` | `bool nullable` |
| `value_i64` | `int64 nullable` |
| `value_f64` | `double nullable` |
| `value_string` | `utf8 nullable` |
| `value_ref` | `uint64 nullable` |

Required sort order:

```text
view_id, type_code, fs_ref
```

## Array Partitions

| column | type |
| --- | --- |
| `array_kind` | `dictionary utf8` |
| `fs_ref` | `uint64` |
| `idx` | `uint32` |
| `value_boolean` | `bool nullable` |
| `value_i64` | `int64 nullable` |
| `value_f64` | `double nullable` |
| `value_string` | `utf8 nullable` |
| `value_ref` | `uint64 nullable` |

Array lengths are stored as rows with `idx = null` in engines that support null
indices, or in a sidecar `array_lengths.parquet` otherwise.
