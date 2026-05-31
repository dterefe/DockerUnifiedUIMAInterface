package org.texttechnologylab.duui.dua.uima.storage;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

public final class DUASqliteCasStorage implements DUAFastCasStorage {
    private static final String SCHEMA_SQL = """
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
            """;
    private final Connection connection;

    public DUASqliteCasStorage(Path path) {
        this("jdbc:sqlite:" + path.toAbsolutePath());
    }

    public DUASqliteCasStorage(String jdbcUrl) {
        try {
            this.connection = DriverManager.getConnection(jdbcUrl);
            initialize();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not open SQLite CAS storage", e);
        }
    }

    private void initialize() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=NORMAL");
            statement.execute("PRAGMA busy_timeout=5000");
            for (String ddl : readSchemaStatements()) {
                String sql = ddl.strip();
                if (!sql.isEmpty()) {
                    statement.execute(sql);
                }
            }
        }
    }

    private static String[] readSchemaStatements() {
        return SCHEMA_SQL.split(";");
    }

    @Override
    public synchronized Optional<DUACasValue> readSlot(int fsRef, String featureName) {
        Integer featureCode = findFeatureId(featureName);
        return featureCode == null ? Optional.empty() : readSlotByFeatureId(fsRef, featureCode);
    }

    @Override
    public synchronized Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        return readSlotByFeatureId(fsRef, featureCode);
    }

    private Optional<DUACasValue> readSlotByFeatureId(int fsRef, int featureId) {
        Optional<DUACasValue> bool = readBoolSlot(fsRef, featureId);
        if (bool.isPresent()) return bool;
        Optional<DUACasValue> i64 = readI64Slot(fsRef, featureId);
        if (i64.isPresent()) return i64;
        Optional<DUACasValue> f32 = readF32Slot(fsRef, featureId);
        if (f32.isPresent()) return f32;
        Optional<DUACasValue> f64 = readF64Slot(fsRef, featureId);
        if (f64.isPresent()) return f64;
        Optional<DUACasValue> string = readStringSlot(fsRef, featureId);
        if (string.isPresent()) return string;
        return readRefSlot(fsRef, featureId);
    }

    @Override
    public synchronized void writeSlot(int fsRef, String featureName, DUACasValue value) {
        int featureId = featureId(featureName);
        writeSlotByFeatureId(fsRef, featureId, value);
    }

    @Override
    public synchronized void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        rememberFeature(featureCode, featureName);
        writeSlotByFeatureId(fsRef, featureCode, value);
    }

    private void writeSlotByFeatureId(int fsRef, int featureId, DUACasValue value) {
        deleteSlotById(fsRef, featureId);
        if (value.value() == null) {
            return;
        }
        switch (value.kind()) {
            case BOOLEAN -> writeBoolSlot(fsRef, featureId, value.booleanValue());
            case BYTE, SHORT, INTEGER, LONG -> writeI64Slot(fsRef, featureId, kindCode(value.kind()), value.longValue());
            case FLOAT -> writeF32Slot(fsRef, featureId, Float.floatToRawIntBits(value.floatValue()));
            case DOUBLE -> writeF64Slot(fsRef, featureId, Double.doubleToRawLongBits(value.doubleValue()));
            case STRING -> writeStringSlot(fsRef, featureId, codeForString(value.stringValue()));
            case REF -> writeRefSlot(fsRef, featureId, value.intValue());
        }
    }

    private Optional<DUACasValue> readBoolSlot(int fsRef, int featureId) {
        return readOneInt("SELECT value_bool FROM dua_slot_bool WHERE fs_ref = ? AND feature_id = ?",
                fsRef, featureId).map(v -> DUACasValue.of(v != 0));
    }

    private Optional<DUACasValue> readI64Slot(int fsRef, int featureId) {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT value_kind_code, value_i64 FROM dua_slot_i64 WHERE fs_ref = ? AND feature_id = ?")) {
            statement.setInt(1, fsRef);
            statement.setInt(2, featureId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(decodeI64(rs.getInt(1), rs.getLong(2)));
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not read typed integer slot", e);
        }
    }

    private Optional<DUACasValue> readF32Slot(int fsRef, int featureId) {
        return readOneInt("SELECT value_bits FROM dua_slot_f32 WHERE fs_ref = ? AND feature_id = ?",
                fsRef, featureId).map(bits -> DUACasValue.of(Float.intBitsToFloat(bits)));
    }

    private Optional<DUACasValue> readF64Slot(int fsRef, int featureId) {
        return readOneLong("SELECT value_bits FROM dua_slot_f64 WHERE fs_ref = ? AND feature_id = ?",
                fsRef, featureId).map(bits -> DUACasValue.of(Double.longBitsToDouble(bits)));
    }

    private Optional<DUACasValue> readStringSlot(int fsRef, int featureId) {
        return readOneInt("SELECT string_code FROM dua_slot_string WHERE fs_ref = ? AND feature_id = ?",
                fsRef, featureId).map(code -> DUACasValue.of(stringForCode(code)));
    }

    private Optional<DUACasValue> readRefSlot(int fsRef, int featureId) {
        return readOneInt("SELECT target_fs_ref FROM dua_slot_ref WHERE fs_ref = ? AND feature_id = ?",
                fsRef, featureId).map(DUACasValue::ref);
    }

    private void writeBoolSlot(int fsRef, int featureId, boolean value) {
        executeUpdate("INSERT INTO dua_slot_bool(fs_ref, feature_id, value_bool) VALUES (?, ?, ?)",
                fsRef, featureId, value ? 1 : 0);
    }

    private void writeI64Slot(int fsRef, int featureId, int kindCode, long value) {
        executeUpdate("INSERT INTO dua_slot_i64(fs_ref, feature_id, value_kind_code, value_i64) VALUES (?, ?, ?, ?)",
                fsRef, featureId, kindCode, value);
    }

    @Override
    public int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue) {
        return readOneLong("SELECT value_i64 FROM dua_slot_i64 WHERE fs_ref = ? AND feature_id = ?",
                fsRef, featureCode).map(Long::intValue).orElse(defaultValue);
    }

    @Override
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        rememberFeature(featureCode, featureName);
        deleteSlotById(fsRef, featureCode);
        writeI64Slot(fsRef, featureCode, kindCode(DUACasValueKind.INTEGER), value);
    }

    private void writeF32Slot(int fsRef, int featureId, int valueBits) {
        executeUpdate("INSERT INTO dua_slot_f32(fs_ref, feature_id, value_bits) VALUES (?, ?, ?)",
                fsRef, featureId, valueBits);
    }

    private void writeF64Slot(int fsRef, int featureId, long valueBits) {
        executeUpdate("INSERT INTO dua_slot_f64(fs_ref, feature_id, value_bits) VALUES (?, ?, ?)",
                fsRef, featureId, valueBits);
    }

    private void writeStringSlot(int fsRef, int featureId, int stringCode) {
        executeUpdate("INSERT INTO dua_slot_string(fs_ref, feature_id, string_code) VALUES (?, ?, ?)",
                fsRef, featureId, stringCode);
    }

    private void writeRefSlot(int fsRef, int featureId, int targetFsRef) {
        executeUpdate("INSERT INTO dua_slot_ref(fs_ref, feature_id, target_fs_ref) VALUES (?, ?, ?)",
                fsRef, featureId, targetFsRef);
    }

    private void deleteSlot(int fsRef, String featureName) {
        Integer featureId = findFeatureId(featureName);
        if (featureId != null) {
            deleteSlotById(fsRef, featureId);
        }
    }

    private void deleteSlotById(int fsRef, int featureId) {
        executeUpdate("DELETE FROM dua_slot_bool WHERE fs_ref = ? AND feature_id = ?", fsRef, featureId);
        executeUpdate("DELETE FROM dua_slot_i64 WHERE fs_ref = ? AND feature_id = ?", fsRef, featureId);
        executeUpdate("DELETE FROM dua_slot_f32 WHERE fs_ref = ? AND feature_id = ?", fsRef, featureId);
        executeUpdate("DELETE FROM dua_slot_f64 WHERE fs_ref = ? AND feature_id = ?", fsRef, featureId);
        executeUpdate("DELETE FROM dua_slot_string WHERE fs_ref = ? AND feature_id = ?", fsRef, featureId);
        executeUpdate("DELETE FROM dua_slot_ref WHERE fs_ref = ? AND feature_id = ?", fsRef, featureId);
    }

    private Integer findFeatureId(String featureName) {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT feature_id FROM dua_features WHERE feature_name = ?
                """)) {
            statement.setString(1, featureName);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : null;
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not find feature id", e);
        }
    }

    private int featureId(String featureName) {
        Integer existing = findFeatureId(featureName);
        if (existing != null) {
            return existing;
        }
        int id = nextCounter("next_feature_id");
        rememberFeature(id, featureName);
        return id;
    }

    private void rememberFeature(int featureId, String featureName) {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT OR IGNORE INTO dua_features(feature_id, feature_name) VALUES (?, ?)
                """)) {
            statement.setInt(1, featureId);
            statement.setString(2, featureName);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not remember feature id", e);
        }
    }

    @Override
    public synchronized void initializeArray(DUACasArrayKind kind, int fsRef, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative");
        }
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO dua_arrays(array_kind_code, fs_ref, length)
                VALUES (?, ?, ?)
                ON CONFLICT(array_kind_code, fs_ref)
                DO UPDATE SET length = max(dua_arrays.length, excluded.length)
                """)) {
            statement.setInt(1, arrayKindCode(kind));
            statement.setInt(2, fsRef);
            statement.setInt(3, length);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not initialize CAS array", e);
        }
    }

    @Override
    public synchronized int arraySize(DUACasArrayKind kind, int fsRef) {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT length FROM dua_arrays WHERE array_kind_code = ? AND fs_ref = ?")) {
            statement.setInt(1, arrayKindCode(kind));
            statement.setInt(2, fsRef);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not read CAS array size", e);
        }
    }

    @Override
    public synchronized Optional<DUACasValue> readArrayValue(DUACasArrayKind kind, int fsRef, int index) {
        checkArrayIndex(kind, fsRef, index);
        int kindCode = arrayKindCode(kind);
        return switch (kind) {
            case BOOLEAN -> readOneInt("SELECT value_bool FROM dua_array_bool WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(v -> DUACasValue.of(v != 0));
            case BYTE -> readOneLong("SELECT value_i64 FROM dua_array_i64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(v -> DUACasValue.of(v.byteValue()));
            case SHORT -> readOneLong("SELECT value_i64 FROM dua_array_i64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(v -> DUACasValue.of(v.shortValue()));
            case INTEGER -> readOneLong("SELECT value_i64 FROM dua_array_i64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(v -> DUACasValue.ofInt(v.intValue()));
            case LONG -> readOneLong("SELECT value_i64 FROM dua_array_i64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(DUACasValue::ofLong);
            case FLOAT -> readOneInt("SELECT value_bits FROM dua_array_f32 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(bits -> DUACasValue.of(Float.intBitsToFloat(bits)));
            case DOUBLE -> readOneLong("SELECT value_bits FROM dua_array_f64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(bits -> DUACasValue.of(Double.longBitsToDouble(bits)));
            case STRING -> readOneInt("SELECT string_code FROM dua_array_string WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(code -> DUACasValue.of(stringForCode(code)));
            case FS -> readOneInt("SELECT target_fs_ref FROM dua_array_ref WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?",
                    kindCode, fsRef, index).map(DUACasValue::ref);
        };
    }

    @Override
    public synchronized void writeArrayValue(DUACasArrayKind kind, int fsRef, int index, DUACasValue value) {
        initializeArray(kind, fsRef, index + 1);
        int kindCode = arrayKindCode(kind);
        deleteArrayValue(kind, fsRef, index);
        if (value.value() == null) {
            return;
        }
        switch (kind) {
            case BOOLEAN -> executeUpdate("INSERT INTO dua_array_bool(array_kind_code, fs_ref, idx, value_bool) VALUES (?, ?, ?, ?)",
                    kindCode, fsRef, index, value.booleanValue() ? 1 : 0);
            case BYTE, SHORT, INTEGER, LONG -> executeUpdate("INSERT INTO dua_array_i64(array_kind_code, fs_ref, idx, value_i64) VALUES (?, ?, ?, ?)",
                    kindCode, fsRef, index, value.longValue());
            case FLOAT -> executeUpdate("INSERT INTO dua_array_f32(array_kind_code, fs_ref, idx, value_bits) VALUES (?, ?, ?, ?)",
                    kindCode, fsRef, index, Float.floatToRawIntBits(value.floatValue()));
            case DOUBLE -> executeUpdate("INSERT INTO dua_array_f64(array_kind_code, fs_ref, idx, value_bits) VALUES (?, ?, ?, ?)",
                    kindCode, fsRef, index, Double.doubleToRawLongBits(value.doubleValue()));
            case STRING -> executeUpdate("INSERT INTO dua_array_string(array_kind_code, fs_ref, idx, string_code) VALUES (?, ?, ?, ?)",
                    kindCode, fsRef, index, codeForString(value.stringValue()));
            case FS -> executeUpdate("INSERT INTO dua_array_ref(array_kind_code, fs_ref, idx, target_fs_ref) VALUES (?, ?, ?, ?)",
                    kindCode, fsRef, index, value.intValue());
        }
    }

    private void deleteArrayValue(DUACasArrayKind kind, int fsRef, int index) {
        int kindCode = arrayKindCode(kind);
        executeUpdate("DELETE FROM dua_array_bool WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?", kindCode, fsRef, index);
        executeUpdate("DELETE FROM dua_array_i64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?", kindCode, fsRef, index);
        executeUpdate("DELETE FROM dua_array_f32 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?", kindCode, fsRef, index);
        executeUpdate("DELETE FROM dua_array_f64 WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?", kindCode, fsRef, index);
        executeUpdate("DELETE FROM dua_array_string WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?", kindCode, fsRef, index);
        executeUpdate("DELETE FROM dua_array_ref WHERE array_kind_code = ? AND fs_ref = ? AND idx = ?", kindCode, fsRef, index);
    }

    private void checkArrayIndex(DUACasArrayKind kind, int fsRef, int index) {
        int size = arraySize(kind, fsRef);
        if (index < 0 || index >= size) {
            throw new ArrayIndexOutOfBoundsException("index " + index + " outside array size " + size);
        }
    }

    @Override
    public synchronized String stringForCode(int code) {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT value FROM dua_strings WHERE code = ?")) {
            statement.setInt(1, code);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not read string code", e);
        }
    }

    @Override
    public synchronized int codeForString(String value) {
        if (value == null) {
            return 0;
        }
        Integer existing = existingStringCode(value);
        if (existing != null) {
            return existing;
        }
        int code = nextCounter("next_string_code");
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO dua_strings(code, value) VALUES (?, ?)")) {
            statement.setInt(1, code);
            statement.setString(2, value);
            statement.executeUpdate();
            return code;
        } catch (SQLException e) {
            Integer raced = existingStringCode(value);
            if (raced != null) {
                return raced;
            }
            throw new DUACasStorageException("Could not write string code", e);
        }
    }

    private Integer existingStringCode(String value) {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT code FROM dua_strings WHERE value = ?")) {
            statement.setString(1, value);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : null;
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not find string code", e);
        }
    }

    @Override
    public synchronized int allocateFsId(int typeCode, int viewId) {
        return nextCounter("next_fs_id");
    }

    @Override
    public synchronized void onFsCreated(int fsRef, int typeCode, int viewId) {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO dua_fs_lifecycle(fs_ref, type_code, view_id, deleted)
                VALUES (?, ?, ?, 0)
                ON CONFLICT(fs_ref)
                DO UPDATE SET type_code = excluded.type_code, view_id = excluded.view_id, deleted = 0
                """)) {
            statement.setInt(1, fsRef);
            statement.setInt(2, typeCode);
            statement.setInt(3, viewId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not record FS creation", e);
        }
    }

    @Override
    public synchronized void onFsDeleted(int fsRef) {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE dua_fs_lifecycle SET deleted = 1 WHERE fs_ref = ?")) {
            statement.setInt(1, fsRef);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not record FS deletion", e);
        }
    }

    private int nextCounter(String key) {
        try {
            connection.setAutoCommit(false);
            int current;
            try (PreparedStatement read = connection.prepareStatement(
                    "SELECT value FROM dua_meta WHERE key = ?")) {
                read.setString(1, key);
                try (ResultSet rs = read.executeQuery()) {
                    current = rs.next() ? Integer.parseInt(rs.getString(1)) : 1;
                }
            }
            try (PreparedStatement write = connection.prepareStatement(
                    "INSERT INTO dua_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")) {
                write.setString(1, key);
                write.setString(2, Integer.toString(current + 1));
                write.executeUpdate();
            }
            connection.commit();
            return current;
        } catch (SQLException e) {
            rollbackQuietly();
            throw new DUACasStorageException("Could not update counter " + key, e);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                throw new DUACasStorageException("Could not restore SQLite auto-commit", e);
            }
        }
    }

    private void rollbackQuietly() {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    private Optional<Integer> readOneInt(String sql, Object... args) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, args);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(rs.getInt(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not read integer value", e);
        }
    }

    private Optional<Long> readOneLong(String sql, Object... args) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, args);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not read long value", e);
        }
    }

    private void executeUpdate(String sql, Object... args) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, args);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not execute storage update", e);
        }
    }

    private static void bind(PreparedStatement statement, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            statement.setObject(i + 1, args[i]);
        }
    }

    private static int kindCode(DUACasValueKind kind) {
        return switch (kind) {
            case BOOLEAN -> 1;
            case BYTE -> 2;
            case SHORT -> 3;
            case INTEGER -> 4;
            case LONG -> 5;
            case FLOAT -> 6;
            case DOUBLE -> 7;
            case STRING -> 8;
            case REF -> 9;
        };
    }

    private static DUACasValue decodeI64(int kindCode, long value) {
        return switch (kindCode) {
            case 2 -> DUACasValue.of((byte) value);
            case 3 -> DUACasValue.of((short) value);
            case 4 -> DUACasValue.ofInt((int) value);
            case 5 -> DUACasValue.ofLong(value);
            default -> throw new DUACasStorageException("Unsupported i64 kind code " + kindCode);
        };
    }

    private static int arrayKindCode(DUACasArrayKind kind) {
        return switch (kind) {
            case FS -> 1;
            case INTEGER -> 2;
            case FLOAT -> 3;
            case STRING -> 4;
            case BOOLEAN -> 5;
            case BYTE -> 6;
            case SHORT -> 7;
            case LONG -> 8;
            case DOUBLE -> 9;
        };
    }

    @Override
    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DUACasStorageException("Could not close SQLite CAS storage", e);
        }
    }
}
