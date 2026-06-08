package org.texttechnologylab.duui.dua.backend.postgres;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUAValueQuery;
import org.texttechnologylab.duui.dua.store.DUAValueQueryStore;
import org.texttechnologylab.duui.dua.store.DUAValueRow;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * PostgreSQL-backed implementation of {@link DUAValueQueryStore}.
 * <p>
 * Stores Feature Structure values and features in relational tables:
 * <ul>
 *   <li>{@code dua_fs_values} — primary FS registry with type code, deletion flag, and creation timestamp</li>
 *   <li>{@code dua_fs_features} — feature key-value store with type-specific columns and indexes</li>
 * </ul>
 * </p>
 */
public final class DUAPostgresValueQueryStore implements DUAValueQueryStore {

    /** Default name for the FS values table. */
    public static final String DEFAULT_FS_TABLE = "dua_fs_values";

    /** Default name for the FS features table. */
    public static final String DEFAULT_FEATURES_TABLE = "dua_fs_features";

    // ── Value type codes stored in the value_type column ──
    private static final short VT_STRING  = 0;
    private static final short VT_INTEGER = 1;
    private static final short VT_LONG    = 2;
    private static final short VT_FLOAT   = 3;
    private static final short VT_DOUBLE  = 4;
    private static final short VT_BOOLEAN = 5;
    private static final short VT_BYTE    = 6;
    private static final short VT_SHORT   = 7;
    private static final short VT_REF     = 8;

    private final DUAPostgresConnectionProvider connections;
    private final String fsTable;
    private final String featuresTable;

    // ── Cached SQL strings (parameterised with table names at construction) ──
    private final String sqlCreateFsValues;
    private final String sqlCreateFsFeatures;
    private final String sqlInsertFs;
    private final String sqlUpsertFeature;
    private final String sqlSelectFeature;
    private final String sqlDeleteFeatures;
    private final String sqlSoftDeleteFs;
    private final String sqlExistsFs;
    private final String sqlTypeCode;
    private final String sqlSelectAllFeaturesByFs;
    private final String sqlSelectFeaturesByCodeAndString;
    private final String sqlSelectFeaturesByCodeAndInt;
    private final String sqlSelectFeaturesByCodeAndFloat;
    private final String sqlSelectFeaturesByCodeAndRef;
    private final String sqlSelectFeaturesByCodeAndRangeInt;
    private final String sqlSelectFeaturesByCodeAndRangeRef;

    /**
     * Constructs a new store using the default table names.
     *
     * @param connections the PostgreSQL connection provider
     */
    public DUAPostgresValueQueryStore(DUAPostgresConnectionProvider connections) {
        this(connections, DEFAULT_FS_TABLE, DEFAULT_FEATURES_TABLE);
    }

    /**
     * Constructs a new store with custom table names.
     *
     * @param connections    the PostgreSQL connection provider
     * @param fsTable        the name of the FS values table
     * @param featuresTable  the name of the FS features table
     */
    public DUAPostgresValueQueryStore(DUAPostgresConnectionProvider connections,
                                      String fsTable,
                                      String featuresTable) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.fsTable = DUAPostgresNames.relation(fsTable);
        this.featuresTable = DUAPostgresNames.relation(featuresTable);

        // Pre-build SQL strings
        this.sqlCreateFsValues = """
                CREATE TABLE IF NOT EXISTS %s (
                    fs_ref BIGSERIAL PRIMARY KEY,
                    type_code INTEGER NOT NULL,
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    created_epoch_ms BIGINT NOT NULL
                )
                """.formatted(this.fsTable);

        this.sqlCreateFsFeatures = """
                CREATE TABLE IF NOT EXISTS %s (
                    fs_ref BIGINT NOT NULL REFERENCES %s(fs_ref),
                    feature_code SMALLINT NOT NULL,
                    value_type SMALLINT NOT NULL,
                    string_value TEXT,
                    int_value INTEGER,
                    float_value REAL,
                    ref_value BIGINT,
                    PRIMARY KEY (fs_ref, feature_code)
                )
                """.formatted(this.featuresTable, this.fsTable);

        this.sqlInsertFs = """
                INSERT INTO %s (type_code, created_epoch_ms)
                VALUES (?, ?)
                RETURNING fs_ref
                """.formatted(this.fsTable);

        this.sqlUpsertFeature = """
                INSERT INTO %s (fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (fs_ref, feature_code) DO UPDATE SET
                    value_type   = EXCLUDED.value_type,
                    string_value = EXCLUDED.string_value,
                    int_value    = EXCLUDED.int_value,
                    float_value  = EXCLUDED.float_value,
                    ref_value    = EXCLUDED.ref_value
                """.formatted(this.featuresTable);

        this.sqlSelectFeature = """
                SELECT value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE fs_ref = ? AND feature_code = ?
                """.formatted(this.featuresTable);

        this.sqlDeleteFeatures = """
                DELETE FROM %s WHERE fs_ref = ?
                """.formatted(this.featuresTable);

        this.sqlSoftDeleteFs = """
                UPDATE %s SET deleted = TRUE, deleted_epoch_ms = ? WHERE fs_ref = ?
                """.formatted(this.fsTable);

        this.sqlExistsFs = """
                SELECT 1 FROM %s WHERE fs_ref = ? AND deleted = FALSE
                """.formatted(this.fsTable);

        this.sqlTypeCode = """
                SELECT type_code FROM %s WHERE fs_ref = ?
                """.formatted(this.fsTable);

        this.sqlSelectAllFeaturesByFs = """
                SELECT feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE fs_ref = ?
                """.formatted(this.featuresTable);

        this.sqlSelectFeaturesByCodeAndString = """
                SELECT fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE feature_code = ? AND string_value = ?
                """.formatted(this.featuresTable);

        this.sqlSelectFeaturesByCodeAndInt = """
                SELECT fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE feature_code = ? AND int_value = ?
                """.formatted(this.featuresTable);

        this.sqlSelectFeaturesByCodeAndFloat = """
                SELECT fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE feature_code = ? AND float_value = ?
                """.formatted(this.featuresTable);

        this.sqlSelectFeaturesByCodeAndRef = """
                SELECT fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE feature_code = ? AND ref_value = ?
                """.formatted(this.featuresTable);

        this.sqlSelectFeaturesByCodeAndRangeInt = """
                SELECT fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE feature_code = ? AND int_value >= ? AND int_value <= ?
                """.formatted(this.featuresTable);

        this.sqlSelectFeaturesByCodeAndRangeRef = """
                SELECT fs_ref, feature_code, value_type, string_value, int_value, float_value, ref_value
                FROM %s
                WHERE feature_code = ? AND ref_value >= ? AND ref_value <= ?
                """.formatted(this.featuresTable);

        ensureTables();
    }

    // ========================================================================
    //  Table initialisation
    // ========================================================================

    private void ensureTables() {
        try (Connection connection = connections.openConnection();
             PreparedStatement stFs = connection.prepareStatement(sqlCreateFsValues);
             PreparedStatement stFeatures = connection.prepareStatement(sqlCreateFsFeatures)) {
            stFs.executeUpdate();
            stFeatures.executeUpdate();

            // Create indexes (IF NOT EXISTS is used so repeated calls are safe)
            // HASH indexes (single-column only, per PG limitation) used instead of
            // btree because string values can exceed btree's max row size (~2.7KB per
            // index entry with v4). PG uses bitmap combine for multi-column equality queries.
            String prefix = featuresTable.replace('.', '_');
            try (PreparedStatement idx1 = connection.prepareStatement(
                    "CREATE INDEX IF NOT EXISTS idx_%s_feature_code ON %s USING BTREE (feature_code)"
                            .formatted(prefix, featuresTable));
                 PreparedStatement idx2 = connection.prepareStatement(
                         "CREATE INDEX IF NOT EXISTS idx_%s_string_value ON %s USING HASH (string_value)"
                                 .formatted(prefix, featuresTable));
                 PreparedStatement idx3 = connection.prepareStatement(
                         "CREATE INDEX IF NOT EXISTS idx_%s_int_value ON %s USING HASH (int_value)"
                                 .formatted(prefix, featuresTable));
                 PreparedStatement idx4 = connection.prepareStatement(
                         "CREATE INDEX IF NOT EXISTS idx_%s_float_value ON %s USING HASH (float_value)"
                                 .formatted(prefix, featuresTable));
                 PreparedStatement idx5 = connection.prepareStatement(
                         "CREATE INDEX IF NOT EXISTS idx_%s_ref_value ON %s USING HASH (ref_value)"
                                 .formatted(prefix, featuresTable))) {
                idx1.executeUpdate();
                idx2.executeUpdate();
                idx3.executeUpdate();
                idx4.executeUpdate();
                idx5.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create PostgreSQL tables for DUA value store", e);
        }
    }

    // ========================================================================
    //  FS Lifecycle
    // ========================================================================

    /**
     * Creates a new feature structure with the given type code.
     *
     * @param typeCode the type code
     * @return the newly allocated fsRef
     */
    public long createFS(int typeCode) {
        if (typeCode < 0) {
            throw new IllegalArgumentException("typeCode must not be negative");
        }
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlInsertFs)) {
            statement.setInt(1, typeCode);
            statement.setLong(2, System.currentTimeMillis());
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                throw new IllegalStateException("INSERT RETURNING did not return a row");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create FS in PostgreSQL", e);
        }
    }

    /**
     * Sets a feature value on the specified FS.
     *
     * @param fsRef       the feature structure reference
     * @param featureCode the feature code
     * @param value       the value to set (may be {@code null})
     */
    public void setFeature(long fsRef, int featureCode, Object value) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlUpsertFeature)) {
            statement.setLong(1, fsRef);
            statement.setInt(2, featureCode);
            bindValue(statement, value);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not set feature in PostgreSQL", e);
        }
    }

    /**
     * Gets a feature value from the specified FS.
     *
     * @param fsRef       the feature structure reference
     * @param featureCode the feature code
     * @return the value, or {@code null} if not set
     */
    public Object getFeature(long fsRef, int featureCode) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlSelectFeature)) {
            statement.setLong(1, fsRef);
            statement.setInt(2, featureCode);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return readValue(rs);
                }
                return null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not get feature from PostgreSQL", e);
        }
    }

    /**
     * Returns an unmodifiable view of all features on the specified FS.
     *
     * @param fsRef the feature structure reference
     * @return map of feature code to value (never {@code null})
     */
    public Map<Integer, Object> getFeatures(long fsRef) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlSelectAllFeaturesByFs)) {
            statement.setLong(1, fsRef);
            try (ResultSet rs = statement.executeQuery()) {
                Map<Integer, Object> result = new LinkedHashMap<>();
                while (rs.next()) {
                    int featureCode = rs.getInt("feature_code");
                    result.put(featureCode, readValue(rs));
                }
                return Collections.unmodifiableMap(result);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not get features from PostgreSQL", e);
        }
    }

    /**
     * Deletes the specified FS and all its features.
     *
     * @param fsRef the feature structure reference
     */
    public void deleteFS(long fsRef) {
        try (Connection connection = connections.openConnection()) {
            connection.setAutoCommit(false);
            try {
                // Soft-delete the FS record
                try (PreparedStatement st = connection.prepareStatement(sqlSoftDeleteFs)) {
                    st.setLong(1, System.currentTimeMillis());
                    st.setLong(2, fsRef);
                    st.executeUpdate();
                }
                // Hard-delete the features
                try (PreparedStatement st = connection.prepareStatement(sqlDeleteFeatures)) {
                    st.setLong(1, fsRef);
                    st.executeUpdate();
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not delete FS from PostgreSQL", e);
        }
    }

    /**
     * Marks the FS as deleted without removing its features.
     *
     * @param fsRef the feature structure reference
     */
    public void markDeleted(long fsRef) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlSoftDeleteFs)) {
            statement.setLong(1, System.currentTimeMillis());
            statement.setLong(2, fsRef);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not mark FS as deleted in PostgreSQL", e);
        }
    }

    /**
     * Checks if an FS exists and is not marked deleted.
     *
     * @param fsRef the feature structure reference
     * @return {@code true} if the FS exists and is not deleted
     */
    public boolean exists(long fsRef) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlExistsFs)) {
            statement.setLong(1, fsRef);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not check FS existence in PostgreSQL", e);
        }
    }

    /**
     * Returns the type code for the given FS.
     *
     * @param fsRef the feature structure reference
     * @return the type code, or -1 if not found
     */
    public int getTypeCode(long fsRef) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlTypeCode)) {
            statement.setLong(1, fsRef);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("type_code");
                }
                return -1;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not get type code from PostgreSQL", e);
        }
    }

    /**
     * Creates multiple FS instances of the same type in a single batch.
     *
     * @param typeCode the type code
     * @param count    the number of FS instances to create
     * @return list of newly allocated fsRefs
     */
    public List<Long> bulkCreateFS(int typeCode, int count) {
        if (typeCode < 0) {
            throw new IllegalArgumentException("typeCode must not be negative");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        List<Long> refs = new ArrayList<>(count);
        long now = System.currentTimeMillis();
        String sql = "INSERT INTO " + fsTable + " (type_code, created_epoch_ms) VALUES (?, ?) RETURNING fs_ref";

        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < count; i++) {
                statement.setInt(1, typeCode);
                statement.setLong(2, now);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        refs.add(rs.getLong(1));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not bulk-create FS in PostgreSQL", e);
        }
        return refs;
    }

    /**
     * Batch create FS instances using JDBC batch with RETURN_GENERATED_KEYS.
     * Executes all inserts in a single executeBatch() call.
     *
     * @param typeCode the type code
     * @param count    the number of FS instances to create
     * @return list of newly allocated fsRefs in the same order as the input
     */
    public List<Long> batchCreateFS(int typeCode, int count) {
        if (typeCode < 0) {
            throw new IllegalArgumentException("typeCode must not be negative");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        long now = System.currentTimeMillis();
        String sql = "INSERT INTO " + fsTable + " (type_code, created_epoch_ms) VALUES (?, ?)";
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                statement.setInt(1, typeCode);
                statement.setLong(2, now);
                statement.addBatch();
            }
            statement.executeBatch();
            List<Long> refs = new ArrayList<>(count);
            try (ResultSet rs = statement.getGeneratedKeys()) {
                while (rs.next()) {
                    refs.add(rs.getLong(1));
                }
            }
            return refs;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not batch-create FS in PostgreSQL", e);
        }
    }

    /**
     * Batch set features for multiple FS/feature combinations in one transaction.
     * Uses INSERT ... ON CONFLICT DO UPDATE with addBatch() + executeBatch().
     *
     * @param features list of feature entries to set
     */
    public void batchSetFeatures(List<FeatureEntry> features) {
        if (features == null || features.isEmpty()) return;
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlUpsertFeature)) {
            for (FeatureEntry f : features) {
                statement.setLong(1, f.fsRef);
                statement.setInt(2, f.featureCode);
                bindValue(statement, f.value);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not batch-set features in PostgreSQL", e);
        }
    }

    /**
     * A feature entry for batch operations, holding an FS reference,
     * feature code, and value.
     */
    public record FeatureEntry(long fsRef, int featureCode, Object value) {}

    // ========================================================================
    //  DUAValueQueryStore implementation
    // ========================================================================

    @Override
    public Stream<DUAValueRow> find(DUAValueQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUAValueQuery.FeatureEquals q -> findByFeatureEquals(q);
            case DUAValueQuery.FeatureRange q -> findByFeatureRange(q);
            case DUAValueQuery.ReferenceTarget q -> findByReferenceTarget(q);
            case DUAValueQuery.CollectionContains q -> findByCollectionContains(q);
        };
    }

    private Stream<DUAValueRow> findByFeatureEquals(DUAValueQuery.FeatureEquals query) {
        int featureCode = query.featureName().hashCode();
        DUACasValue searchValue = query.value();
        Object raw = searchValue.value();

        Stream<FeatureRow> candidates;
        if (raw instanceof String s) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndString, statement -> {
                statement.setInt(1, featureCode);
                statement.setString(2, s);
            });
        } else if (raw instanceof Boolean b) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndInt, statement -> {
                statement.setInt(1, featureCode);
                statement.setInt(2, b ? 1 : 0);
            });
        } else if (raw instanceof Byte b) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndInt, statement -> {
                statement.setInt(1, featureCode);
                statement.setInt(2, b.intValue());
            });
        } else if (raw instanceof Short s) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndInt, statement -> {
                statement.setInt(1, featureCode);
                statement.setInt(2, s.intValue());
            });
        } else if (raw instanceof Integer i) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndInt, statement -> {
                statement.setInt(1, featureCode);
                statement.setInt(2, i);
            });
        } else if (raw instanceof Long l) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndRef, statement -> {
                statement.setInt(1, featureCode);
                statement.setLong(2, l);
            });
        } else if (raw instanceof Float f) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndFloat, statement -> {
                statement.setInt(1, featureCode);
                statement.setFloat(2, f);
            });
        } else if (raw instanceof Double d) {
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndFloat, statement -> {
                statement.setInt(1, featureCode);
                statement.setDouble(2, d);
            });
        } else {
            // Fallback: string search on toString()
            candidates = queryFeatures(sqlSelectFeaturesByCodeAndString, statement -> {
                statement.setInt(1, featureCode);
                statement.setString(2, raw == null ? null : raw.toString());
            });
        }

        Stream<FeatureRow> filtered = candidates;
        if (query.typeId().isPresent()) {
            int typeId = query.typeId().getAsInt();
            filtered = candidates.filter(fr -> getTypeCode(fr.fsRef()) == typeId);
        }

        return filtered.map(fr -> toRow(query.casId(), query.viewId(), fr, query.featureName()));
    }

    private Stream<DUAValueRow> findByFeatureRange(DUAValueQuery.FeatureRange query) {
        int featureCode = query.featureName().hashCode();
        long lower = query.lowerInclusive();
        long upper = query.upperInclusive();

        // Search both int_value and ref_value columns
        Stream<FeatureRow> fromInt = queryFeatures(sqlSelectFeaturesByCodeAndRangeInt, statement -> {
            statement.setInt(1, featureCode);
            statement.setLong(2, lower);
            statement.setLong(3, upper);
        });
        Stream<FeatureRow> fromRef = queryFeatures(sqlSelectFeaturesByCodeAndRangeRef, statement -> {
            statement.setInt(1, featureCode);
            statement.setLong(2, lower);
            statement.setLong(3, upper);
        });

        Stream<FeatureRow> combined = Stream.concat(fromInt, fromRef).distinct();

        if (query.typeId().isPresent()) {
            int typeId = query.typeId().getAsInt();
            combined = combined.filter(fr -> getTypeCode(fr.fsRef()) == typeId);
        }

        return combined.map(fr -> toRow(query.casId(), query.viewId(), fr, query.featureName()));
    }

    private Stream<DUAValueRow> findByReferenceTarget(DUAValueQuery.ReferenceTarget query) {
        int featureCode = query.featureName().hashCode();
        long targetFsRef = query.targetFsRef();

        Stream<FeatureRow> candidates = queryFeatures(sqlSelectFeaturesByCodeAndRef, statement -> {
            statement.setInt(1, featureCode);
            statement.setLong(2, targetFsRef);
        });

        return candidates
                .filter(fr -> exists(fr.fsRef()))
                .map(fr -> toRow(query.casId(), query.viewId(), fr, query.featureName()));
    }

    private Stream<DUAValueRow> findByCollectionContains(DUAValueQuery.CollectionContains query) {
        long collectionFsRef = query.collectionFsRef();
        DUACasValue searchValue = query.value();
        Object raw = searchValue.value();

        // Load all features of the collection FS and filter in-memory
        return queryFeatures(sqlSelectAllFeaturesByFs, statement -> {
            statement.setLong(1, collectionFsRef);
        }).filter(fr -> valuesMatch(raw, fr))
          .map(fr -> toRow(query.casId(), query.viewId(), fr, String.valueOf(fr.featureCode())));
    }

    // ========================================================================
    //  Internal helpers
    // ========================================================================

    /**
     * Binds a value to a PreparedStatement for the upsert_feature SQL.
     * Parameters 3-7 correspond to: value_type, string_value, int_value, float_value, ref_value
     */
    private void bindValue(PreparedStatement statement, Object value) throws SQLException {
        if (value == null) {
            statement.setShort(3, VT_STRING);
            statement.setNull(4, Types.VARCHAR);
            statement.setNull(5, Types.INTEGER);
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
            return;
        }

        if (value instanceof String s) {
            statement.setShort(3, VT_STRING);
            statement.setString(4, s);
            statement.setNull(5, Types.INTEGER);
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
        } else if (value instanceof Boolean b) {
            statement.setShort(3, VT_BOOLEAN);
            statement.setNull(4, Types.VARCHAR);
            statement.setInt(5, b ? 1 : 0);
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
        } else if (value instanceof Byte b) {
            statement.setShort(3, VT_BYTE);
            statement.setNull(4, Types.VARCHAR);
            statement.setInt(5, b.intValue());
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
        } else if (value instanceof Short s) {
            statement.setShort(3, VT_SHORT);
            statement.setNull(4, Types.VARCHAR);
            statement.setInt(5, s.intValue());
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
        } else if (value instanceof Integer i) {
            statement.setShort(3, VT_INTEGER);
            statement.setNull(4, Types.VARCHAR);
            statement.setInt(5, i);
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
        } else if (value instanceof Long l) {
            statement.setShort(3, VT_LONG);
            statement.setNull(4, Types.VARCHAR);
            statement.setNull(5, Types.INTEGER);
            statement.setNull(6, Types.REAL);
            statement.setLong(7, l);
        } else if (value instanceof Float f) {
            statement.setShort(3, VT_FLOAT);
            statement.setNull(4, Types.VARCHAR);
            statement.setNull(5, Types.INTEGER);
            statement.setFloat(6, f);
            statement.setNull(7, Types.BIGINT);
        } else if (value instanceof Double d) {
            statement.setShort(3, VT_DOUBLE);
            statement.setNull(4, Types.VARCHAR);
            statement.setNull(5, Types.INTEGER);
            statement.setDouble(6, d);
            statement.setNull(7, Types.BIGINT);
        } else {
            // Fallback: store as string
            statement.setShort(3, VT_STRING);
            statement.setString(4, value.toString());
            statement.setNull(5, Types.INTEGER);
            statement.setNull(6, Types.REAL);
            statement.setNull(7, Types.BIGINT);
        }
    }

    /**
     * Reads a value from the current row of a ResultSet based on value_type.
     */
    private Object readValue(ResultSet rs) throws SQLException {
        short valueType = rs.getShort("value_type");
        return switch (valueType) {
            case VT_STRING  -> rs.getString("string_value");
            case VT_BOOLEAN -> rs.getInt("int_value") != 0;
            case VT_BYTE    -> (byte) rs.getInt("int_value");
            case VT_SHORT   -> (short) rs.getInt("int_value");
            case VT_INTEGER -> rs.getInt("int_value");
            case VT_LONG    -> rs.getLong("ref_value");
            case VT_FLOAT   -> rs.getFloat("float_value");
            case VT_DOUBLE  -> rs.getDouble("float_value");
            case VT_REF     -> rs.getLong("ref_value");
            default         -> rs.getString("string_value");
        };
    }

    /**
     * Checks whether a raw search value conceptually matches a stored FeatureRow.
     */
    private boolean valuesMatch(Object searchRaw, FeatureRow fr) {
        if (searchRaw == null) {
            return fr.stringValue() == null;
        }
        return switch (fr.valueType()) {
            case VT_STRING  -> searchRaw instanceof String s && s.equals(fr.stringValue());
            case VT_BOOLEAN -> searchRaw instanceof Boolean b && b.equals(fr.intValue() != 0);
            case VT_BYTE    -> searchRaw instanceof Byte b && b == (byte) fr.intValue();
            case VT_SHORT   -> searchRaw instanceof Short s && s == (short) fr.intValue();
            case VT_INTEGER -> searchRaw instanceof Integer i && i.equals(fr.intValue());
            case VT_LONG    -> searchRaw instanceof Long l && l.equals(fr.refValue());
            case VT_FLOAT   -> searchRaw instanceof Float f && f.equals(fr.floatValue());
            case VT_DOUBLE  -> searchRaw instanceof Double d && d.equals(fr.floatValue());
            case VT_REF     -> searchRaw instanceof Long l && l.equals(fr.refValue())
                    || searchRaw instanceof Integer i && i.longValue() == fr.refValue();
            default         -> false;
        };
    }

    /**
     * Queries the features table and returns matching rows as a stream of {@link FeatureRow}.
     * <p>
     * JDBC resources are safely closed via try-with-resources after eager collection.
     * </p>
     */
    private Stream<FeatureRow> queryFeatures(String sql, Binder binder) {
        List<FeatureRow> rows;
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(new FeatureRow(
                            resultSet.getLong("fs_ref"),
                            resultSet.getInt("feature_code"),
                            resultSet.getShort("value_type"),
                            resultSet.getString("string_value"),
                            resultSet.getInt("int_value"),
                            resultSet.getFloat("float_value"),
                            resultSet.getLong("ref_value")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not query features in PostgreSQL", e);
        }
        return rows.stream();
    }

    /**
     * Converts a database {@link FeatureRow} into a {@link DUAValueRow}.
     */
    private DUAValueRow toRow(DUAId casId, DUAId viewId, FeatureRow fr, String featureName) {
        DUACasValue cv = toDuacValue(fr);
        return new DUAValueRow(casId, viewId, fr.fsRef(), fr.featureCode(), featureName, cv);
    }

    /**
     * Converts a database {@link FeatureRow} into a {@link DUACasValue}.
     */
    private DUACasValue toDuacValue(FeatureRow fr) {
        return switch (fr.valueType()) {
            case VT_STRING  -> DUACasValue.of(fr.stringValue() != null ? fr.stringValue() : "");
            case VT_BOOLEAN -> DUACasValue.of(fr.intValue() != 0);
            case VT_BYTE    -> DUACasValue.of((byte) fr.intValue());
            case VT_SHORT   -> DUACasValue.of((short) fr.intValue());
            case VT_INTEGER -> DUACasValue.ofInt(fr.intValue());
            case VT_LONG    -> DUACasValue.ofLong(fr.refValue());
            case VT_FLOAT   -> DUACasValue.of(fr.floatValue());
            case VT_DOUBLE  -> DUACasValue.of((double) fr.floatValue());
            case VT_REF     -> DUACasValue.ref((int) fr.refValue());
            default         -> DUACasValue.of("");
        };
    }

    // ========================================================================
    //  Internal record
    // ========================================================================

    /**
     * Lightweight record mirroring a row from {@code dua_fs_features}.
     */
    private record FeatureRow(
            long fsRef,
            int featureCode,
            short valueType,
            String stringValue,
            int intValue,
            float floatValue,
            long refValue
    ) {
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
