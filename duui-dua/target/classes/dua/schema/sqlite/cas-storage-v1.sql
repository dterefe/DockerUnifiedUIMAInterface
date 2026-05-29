-- DUA CAS Storage Schema v1
--
-- Purpose:
--   Typed, identifier-first CAS/JCas storage for lazy DUA backends.
--   Hot primitive/reference data is not stringified.
--
-- Physical encoding:
--   BOOLEAN      -> INTEGER 0/1
--   BYTE/SHORT/INTEGER/LONG -> INTEGER
--   FLOAT        -> INTEGER raw Float.floatToRawIntBits(value)
--   DOUBLE       -> INTEGER raw Double.doubleToRawLongBits(value)
--   STRING       -> INTEGER string dictionary code
--   REF          -> INTEGER target fs_ref
--
-- Integer kind codes:
--   1 BOOLEAN, 2 BYTE, 3 SHORT, 4 INTEGER, 5 LONG,
--   6 FLOAT, 7 DOUBLE, 8 STRING, 9 REF
--
-- Array kind codes:
--   1 FS, 2 INTEGER, 3 FLOAT, 4 STRING, 5 BOOLEAN,
--   6 BYTE, 7 SHORT, 8 LONG, 9 DOUBLE

CREATE TABLE IF NOT EXISTS dua_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dua_features (
    feature_id INTEGER PRIMARY KEY,
    feature_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dua_fs_lifecycle (
    fs_ref INTEGER PRIMARY KEY,
    type_code INTEGER NOT NULL,
    view_id INTEGER NOT NULL,
    created_epoch_ms INTEGER,
    deleted INTEGER NOT NULL DEFAULT 0,
    deleted_epoch_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_dua_fs_lifecycle_type_view
    ON dua_fs_lifecycle(type_code, view_id, deleted);

CREATE TABLE IF NOT EXISTS dua_slot_bool (
    fs_ref INTEGER NOT NULL,
    feature_id INTEGER NOT NULL,
    value_bool INTEGER NOT NULL CHECK (value_bool IN (0, 1)),
    updated_epoch_ms INTEGER,
    PRIMARY KEY (fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_slot_bool_feature
    ON dua_slot_bool(feature_id, fs_ref);

CREATE TABLE IF NOT EXISTS dua_slot_i64 (
    fs_ref INTEGER NOT NULL,
    feature_id INTEGER NOT NULL,
    value_kind_code INTEGER NOT NULL CHECK (value_kind_code IN (2, 3, 4, 5)),
    value_i64 INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_slot_i64_feature_value
    ON dua_slot_i64(feature_id, value_i64, fs_ref);

CREATE TABLE IF NOT EXISTS dua_slot_f32 (
    fs_ref INTEGER NOT NULL,
    feature_id INTEGER NOT NULL,
    value_bits INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_slot_f32_feature_bits
    ON dua_slot_f32(feature_id, value_bits, fs_ref);

CREATE TABLE IF NOT EXISTS dua_slot_f64 (
    fs_ref INTEGER NOT NULL,
    feature_id INTEGER NOT NULL,
    value_bits INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_slot_f64_feature_bits
    ON dua_slot_f64(feature_id, value_bits, fs_ref);

CREATE TABLE IF NOT EXISTS dua_slot_string (
    fs_ref INTEGER NOT NULL,
    feature_id INTEGER NOT NULL,
    string_code INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_slot_string_feature_code
    ON dua_slot_string(feature_id, string_code, fs_ref);

CREATE TABLE IF NOT EXISTS dua_slot_ref (
    fs_ref INTEGER NOT NULL,
    feature_id INTEGER NOT NULL,
    target_fs_ref INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_slot_ref_feature_target
    ON dua_slot_ref(feature_id, target_fs_ref, fs_ref);

CREATE INDEX IF NOT EXISTS idx_dua_slot_ref_target
    ON dua_slot_ref(target_fs_ref, fs_ref, feature_id);

CREATE TABLE IF NOT EXISTS dua_arrays (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code BETWEEN 1 AND 9),
    fs_ref INTEGER NOT NULL,
    length INTEGER NOT NULL CHECK (length >= 0),
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref)
);

CREATE TABLE IF NOT EXISTS dua_array_bool (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code = 5),
    fs_ref INTEGER NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    value_bool INTEGER NOT NULL CHECK (value_bool IN (0, 1)),
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref, idx)
);

CREATE TABLE IF NOT EXISTS dua_array_i64 (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code IN (2, 6, 7, 8)),
    fs_ref INTEGER NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    value_i64 INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref, idx)
);

CREATE TABLE IF NOT EXISTS dua_array_f32 (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code = 3),
    fs_ref INTEGER NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    value_bits INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref, idx)
);

CREATE TABLE IF NOT EXISTS dua_array_f64 (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code = 9),
    fs_ref INTEGER NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    value_bits INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref, idx)
);

CREATE TABLE IF NOT EXISTS dua_array_string (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code = 4),
    fs_ref INTEGER NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    string_code INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref, idx)
);

CREATE TABLE IF NOT EXISTS dua_array_ref (
    array_kind_code INTEGER NOT NULL CHECK (array_kind_code = 1),
    fs_ref INTEGER NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    target_fs_ref INTEGER NOT NULL,
    updated_epoch_ms INTEGER,
    PRIMARY KEY (array_kind_code, fs_ref, idx)
);

CREATE INDEX IF NOT EXISTS idx_dua_array_ref_target
    ON dua_array_ref(target_fs_ref, fs_ref, idx);

CREATE TABLE IF NOT EXISTS dua_strings (
    code INTEGER PRIMARY KEY,
    value TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_dua_strings_value
    ON dua_strings(value);

INSERT OR IGNORE INTO dua_meta(key, value) VALUES ('schema_name', 'cas-storage');
INSERT OR IGNORE INTO dua_meta(key, value) VALUES ('schema_version', '1');
INSERT OR IGNORE INTO dua_meta(key, value) VALUES ('next_fs_id', '1');
INSERT OR IGNORE INTO dua_meta(key, value) VALUES ('next_string_code', '1');
INSERT OR IGNORE INTO dua_meta(key, value) VALUES ('next_feature_id', '1');
