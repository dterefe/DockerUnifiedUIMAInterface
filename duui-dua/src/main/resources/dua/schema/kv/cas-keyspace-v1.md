# DUA CAS Key-Value Keyspace v1

Target engines: RocksDB, FoundationDB, LMDB, Badger-style LSM/KV stores.

All numeric segments are big-endian fixed-width encodings so lexical key order
matches numeric order. Hot values are binary, not JSON strings.

## Prefixes

| prefix | record |
| --- | --- |
| `0x01` | feature-structure lifecycle |
| `0x02` | boolean slot |
| `0x03` | integer slot |
| `0x04` | float slot |
| `0x05` | double slot |
| `0x06` | string slot |
| `0x07` | reference slot |
| `0x08` | array metadata |
| `0x09` | boolean array element |
| `0x0A` | integer array element |
| `0x0B` | float array element |
| `0x0C` | double array element |
| `0x0D` | string array element |
| `0x0E` | reference array element |
| `0x0F` | string code to UTF-8 |
| `0x10` | string UTF-8/hash to code |
| `0x11` | counters |
| `0x12` | reverse reference index |

## Shared Encodings

```text
u8       one unsigned byte
u16      two-byte unsigned big-endian
u32      four-byte unsigned big-endian
u64      eight-byte unsigned big-endian
i64      eight-byte signed big-endian
f32bits  four-byte raw IEEE 754 bits
f64bits  eight-byte raw IEEE 754 bits
utf8     raw UTF-8 bytes
```

Feature identity:

```text
feature_hash:u64 = xxh3_64(feature_name)
```

The full feature name is stored in a cold dictionary/index, not repeated in hot
slot keys.

## Lifecycle

Key:

```text
01 | fs_ref:u64
```

Value:

```text
type_code:i32 | view_id:i32 | flags:u8
```

`flags bit 0 = deleted`.

## Slots

Boolean:

```text
key   = 02 | fs_ref:u64 | feature_hash:u64
value = u8  -- 0 or 1
```

Integer family:

```text
key   = 03 | fs_ref:u64 | feature_hash:u64
value = kind:u8 | i64
```

`kind`: `2 BYTE`, `3 SHORT`, `4 INTEGER`, `5 LONG`.

Float:

```text
key   = 04 | fs_ref:u64 | feature_hash:u64
value = f32bits
```

Double:

```text
key   = 05 | fs_ref:u64 | feature_hash:u64
value = f64bits
```

String:

```text
key   = 06 | fs_ref:u64 | feature_hash:u64
value = string_code:u64
```

Reference:

```text
key   = 07 | fs_ref:u64 | feature_hash:u64
value = target_fs_ref:u64
```

Reverse reference index:

```text
key   = 12 | target_fs_ref:u64 | source_fs_ref:u64 | feature_hash:u64
value = empty
```

## Arrays

Array metadata:

```text
key   = 08 | array_kind:u8 | fs_ref:u64
value = length:u32
```

Boolean element:

```text
key   = 09 | array_kind:u8 | fs_ref:u64 | idx:u32
value = u8
```

Integer element:

```text
key   = 0A | array_kind:u8 | fs_ref:u64 | idx:u32
value = i64
```

Float element:

```text
key   = 0B | array_kind:u8 | fs_ref:u64 | idx:u32
value = f32bits
```

Double element:

```text
key   = 0C | array_kind:u8 | fs_ref:u64 | idx:u32
value = f64bits
```

String element:

```text
key   = 0D | array_kind:u8 | fs_ref:u64 | idx:u32
value = string_code:u64
```

Reference element:

```text
key   = 0E | array_kind:u8 | fs_ref:u64 | idx:u32
value = target_fs_ref:u64
```

## Strings

Code to value:

```text
key   = 0F | string_code:u64
value = utf8
```

Value to code:

```text
key   = 10 | string_hash:u64 | utf8
value = string_code:u64
```

## Counters

```text
key   = 11 | counter_name:utf8
value = next:u64
```

Required counters:

- `next_fs_id`
- `next_string_code`

## Array Kind Codes

| code | kind |
| --- | --- |
| `0x01` | `FS` |
| `0x02` | `INTEGER` |
| `0x03` | `FLOAT` |
| `0x04` | `STRING` |
| `0x05` | `BOOLEAN` |
| `0x06` | `BYTE` |
| `0x07` | `SHORT` |
| `0x08` | `LONG` |
| `0x09` | `DOUBLE` |
