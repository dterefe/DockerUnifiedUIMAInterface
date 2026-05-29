# DUA Object/Blob Payload Layout v1

Target engines: ZIP entries, filesystem directories, S3/MinIO, Google Drive-like
object stores.

Payload storage is stream-first. CAS slots should reference payload ids, not
inline large binary data.

## Object Keys

```text
payloads/v1/{payload_id}/data
payloads/v1/{payload_id}/meta.json
payloads/v1/{payload_id}/checksums/{algorithm}.txt
payloads/v1/{payload_id}/ranges/{range_id}.json
```

## `meta.json`

```json
{
  "schema": "dua.payload.v1",
  "payloadId": "01...",
  "mediaType": "text/plain; charset=utf-8",
  "byteLength": 1234,
  "createdEpochMs": 0,
  "contentEncoding": null,
  "source": {
    "kind": "import",
    "uri": "file:///input/doc.txt"
  }
}
```

## Range Descriptor

Used for lazy sub-document or multimodal segment materialization.

```json
{
  "schema": "dua.payload.range.v1",
  "payloadId": "01...",
  "rangeId": "01...",
  "unit": "byte",
  "begin": 0,
  "end": 512,
  "label": "page-1"
}
```
