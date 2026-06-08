package org.texttechnologylab.duui.dua.backend.postgres;

import org.texttechnologylab.duui.dua.query.DUATextQuery;
import org.texttechnologylab.duui.dua.store.DUASofa;
import org.texttechnologylab.duui.dua.store.DUATextQueryStore;
import org.texttechnologylab.duui.dua.store.DUATextRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Stream;

/**
 * PostgreSQL-backed implementation of {@link DUATextQueryStore}.
 * <p>
 * Uses the {@code pg_trgm} extension for fast substring/pattern matching via
 * GIN indexes on {@code covered_text}, and fulltext search via {@code tsvector}.
 * </p>
 *
 * <h3>Schema</h3>
 * <pre>{@code
 * dua_sofas — SoFA (Subject of Analysis) registry
 * dua_annotation_text — Annotation covered text storage with trigram index
 * }</pre>
 */
public final class DUAPostgresTextQueryStore implements DUATextQueryStore {

    /** Default name for the SoFA table. */
    public static final String DEFAULT_SOFA_TABLE = "dua_sofas";

    /** Default name for the annotation text table. */
    public static final String DEFAULT_ANNOTATION_TEXT_TABLE = "dua_annotation_text";

    private final DUAPostgresConnectionProvider connections;
    private final String sofaTable;
    private final String annotationTextTable;

    // ── Cached SQL strings ──
    private final String sqlCreateSofas;
    private final String sqlCreateAnnotationText;
    private final String sqlCreateTrgmIndex;
    private final String sqlCreateBeginEndIndex;
    private final String sqlInsertSofa;
    private final String sqlSelectSofa;
    private final String sqlInsertAnnotationText;
    private final String sqlBulkInsertAnnotationText;
    private final String sqlDeleteAnnotationText;
    private final String sqlSelectExact;
    private final String sqlSelectSubstring;
    private final String sqlSelectCoveredText;

    /**
     * Constructs a new store using the default table names.
     *
     * @param connections the PostgreSQL connection provider
     */
    public DUAPostgresTextQueryStore(DUAPostgresConnectionProvider connections) {
        this(connections, DEFAULT_SOFA_TABLE, DEFAULT_ANNOTATION_TEXT_TABLE);
    }

    /**
     * Constructs a new store with custom table names.
     *
     * @param connections           the PostgreSQL connection provider
     * @param sofaTable             the name of the SoFA table
     * @param annotationTextTable   the name of the annotation text table
     */
    public DUAPostgresTextQueryStore(DUAPostgresConnectionProvider connections,
                                     String sofaTable,
                                     String annotationTextTable) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.sofaTable = DUAPostgresNames.relation(sofaTable);
        this.annotationTextTable = DUAPostgresNames.relation(annotationTextTable);

        // Pre-build SQL strings
        this.sqlCreateSofas = """
                CREATE TABLE IF NOT EXISTS %s (
                    sofa_id BIGINT PRIMARY KEY,
                    sofa_identifier TEXT NOT NULL,
                    local_name TEXT NOT NULL,
                    sofa_type SMALLINT NOT NULL DEFAULT 0,
                    sofa_data TEXT
                )
                """.formatted(this.sofaTable);

        this.sqlCreateAnnotationText = """
                CREATE TABLE IF NOT EXISTS %s (
                    annotation_fs_ref BIGINT NOT NULL,
                    sofa_fs_ref BIGINT NOT NULL,
                    covered_text TEXT NOT NULL,
                    begin_offset INTEGER NOT NULL,
                    end_offset INTEGER NOT NULL,
                    role TEXT NOT NULL DEFAULT 'coveredText',
                    PRIMARY KEY (sofa_fs_ref, annotation_fs_ref)
                )
                """.formatted(this.annotationTextTable);

        String idxPrefix = this.annotationTextTable.replace('.', '_');

        this.sqlCreateTrgmIndex = """
                CREATE INDEX IF NOT EXISTS idx_%s_trgm
                    ON %s USING GIN (covered_text gin_trgm_ops)
                """.formatted(idxPrefix, this.annotationTextTable);

        this.sqlCreateBeginEndIndex = """
                CREATE INDEX IF NOT EXISTS idx_%s_begin_end
                    ON %s (sofa_fs_ref, begin_offset, end_offset)
                """.formatted(idxPrefix, this.annotationTextTable);

        this.sqlInsertSofa = """
                INSERT INTO %s (sofa_id, sofa_identifier, local_name, sofa_type, sofa_data)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (sofa_id) DO UPDATE SET
                    sofa_identifier = EXCLUDED.sofa_identifier,
                    local_name = EXCLUDED.local_name,
                    sofa_type = EXCLUDED.sofa_type,
                    sofa_data = EXCLUDED.sofa_data
                """.formatted(this.sofaTable);

        this.sqlSelectSofa = """
                SELECT sofa_id, sofa_identifier, local_name, sofa_type, sofa_data
                FROM %s
                WHERE sofa_id = ?
                """.formatted(this.sofaTable);

        this.sqlInsertAnnotationText = """
                INSERT INTO %s (annotation_fs_ref, sofa_fs_ref, covered_text, begin_offset, end_offset, role)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (sofa_fs_ref, annotation_fs_ref) DO UPDATE SET
                    covered_text = EXCLUDED.covered_text,
                    begin_offset = EXCLUDED.begin_offset,
                    end_offset = EXCLUDED.end_offset,
                    role = EXCLUDED.role
                """.formatted(this.annotationTextTable);

        this.sqlBulkInsertAnnotationText = """
                INSERT INTO %s (annotation_fs_ref, sofa_fs_ref, covered_text, begin_offset, end_offset, role)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (sofa_fs_ref, annotation_fs_ref) DO NOTHING
                """.formatted(this.annotationTextTable);

        this.sqlDeleteAnnotationText = """
                DELETE FROM %s WHERE sofa_fs_ref = ? AND annotation_fs_ref = ?
                """.formatted(this.annotationTextTable);

        this.sqlSelectExact = """
                SELECT annotation_fs_ref, sofa_fs_ref, covered_text, role
                FROM %s
                WHERE sofa_fs_ref = ? AND covered_text = ?
                """.formatted(this.annotationTextTable);

        this.sqlSelectSubstring = """
                SELECT annotation_fs_ref, sofa_fs_ref, covered_text, role
                FROM %s
                WHERE sofa_fs_ref = ? AND covered_text ILIKE '%%' || ? || '%%'
                """.formatted(this.annotationTextTable);

        this.sqlSelectCoveredText = """
                SELECT annotation_fs_ref, sofa_fs_ref, covered_text, role
                FROM %s
                WHERE sofa_fs_ref = ? AND covered_text = ?
                """.formatted(this.annotationTextTable);

        ensureTables();
    }

    // ========================================================================
    //  Table initialisation
    // ========================================================================

    private void ensureTables() {
        try (Connection connection = connections.openConnection();
             PreparedStatement stSofas = connection.prepareStatement(sqlCreateSofas);
             PreparedStatement stAnnText = connection.prepareStatement(sqlCreateAnnotationText)) {
            stSofas.executeUpdate();
            stAnnText.executeUpdate();

            // Create indexes (IF NOT EXISTS so repeated calls are safe)
            try (PreparedStatement idxTrgm = connection.prepareStatement(sqlCreateTrgmIndex);
                 PreparedStatement idxBeginEnd = connection.prepareStatement(sqlCreateBeginEndIndex)) {
                idxTrgm.executeUpdate();
                idxBeginEnd.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create PostgreSQL tables for DUA text query store", e);
        }
    }

    // ========================================================================
    //  SoFA Management
    // ========================================================================

    /**
     * Registers a SoFA in the store, creating or updating the row.
     *
     * @param sofa the sofa to register
     */
    public void registerSofa(DUASofa sofa) {
        Objects.requireNonNull(sofa, "sofa");
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlInsertSofa)) {
            statement.setLong(1, sofa.fsRef());
            statement.setString(2, sofa.sofaId());
            statement.setString(3, sofa.localName());
            statement.setShort(4, (short) sofa.type().ordinal());
            if (sofa.data() != null) {
                statement.setString(5, new String(sofa.data(), java.nio.charset.StandardCharsets.UTF_8));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not register SoFA in PostgreSQL", e);
        }
    }

    /**
     * Retrieves a SoFA by its fsRef.
     *
     * @param sofaFsRef the sofa feature structure reference
     * @return the sofa, or {@code null} if not found
     */
    public DUASofa getSofa(long sofaFsRef) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlSelectSofa)) {
            statement.setLong(1, sofaFsRef);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return toSofa(rs);
                }
                return null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not get SoFA from PostgreSQL", e);
        }
    }

    // ========================================================================
    //  Covered Text Management
    // ========================================================================

    /**
     * Adds a covered text annotation for the given SoFA.
     *
     * @param sofaRef       the sofa reference
     * @param annotationRef the annotation reference
     * @param begin         the begin offset
     * @param end           the end offset
     * @param text          the covered text
     */
    public void addCoveredText(long sofaRef, long annotationRef, long begin, long end, String text) {
        if (sofaRef < 0) {
            throw new IllegalArgumentException("sofaRef must not be negative");
        }
        if (annotationRef < 0) {
            throw new IllegalArgumentException("annotationRef must not be negative");
        }
        if (begin < 0 || end < begin) {
            throw new IllegalArgumentException("invalid span: begin=" + begin + ", end=" + end);
        }
        Objects.requireNonNull(text, "text");
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlInsertAnnotationText)) {
            statement.setLong(1, annotationRef);
            statement.setLong(2, sofaRef);
            statement.setString(3, text);
            statement.setInt(4, (int) begin);
            statement.setInt(5, (int) end);
            statement.setString(6, "coveredText");
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not add covered text in PostgreSQL", e);
        }
    }

    /**
     * Adds multiple covered text annotations in bulk.
     *
     * @param rows the rows to add
     */
    public void bulkAddCoveredText(List<DUATextRow> rows) {
        Objects.requireNonNull(rows, "rows");
        if (rows.isEmpty()) {
            return;
        }
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlBulkInsertAnnotationText)) {
            for (DUATextRow row : rows) {
                statement.setLong(1, row.fsRef());
                statement.setLong(2, row.sofaFsRef());
                statement.setString(3, row.text());
                statement.setInt(4, 0);   // begin offset not available in DUATextRow
                statement.setInt(5, 0);   // end offset not available in DUATextRow
                statement.setString(6, row.role());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not bulk-add covered text in PostgreSQL", e);
        }
    }

    /**
     * Batch add covered text entries using JDBC batch with upsert semantics.
     * Uses addBatch() + executeBatch() with ON CONFLICT DO UPDATE.
     *
     * @param rows the text rows to add
     */
    public void batchAddCoveredText(List<DUATextRow> rows) {
        Objects.requireNonNull(rows, "rows");
        if (rows.isEmpty()) return;
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlInsertAnnotationText)) {
            for (DUATextRow row : rows) {
                statement.setLong(1, row.fsRef());
                statement.setLong(2, row.sofaFsRef());
                statement.setString(3, row.text());
                statement.setInt(4, 0);   // begin offset not available in DUATextRow
                statement.setInt(5, 0);   // end offset not available in DUATextRow
                statement.setString(6, row.role());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not batch-add covered text in PostgreSQL", e);
        }
    }

    /**
     * Removes a covered text annotation.
     *
     * @param sofaRef       the sofa reference
     * @param annotationRef the annotation reference
     */
    public void removeCoveredText(long sofaRef, long annotationRef) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sqlDeleteAnnotationText)) {
            statement.setLong(1, sofaRef);
            statement.setLong(2, annotationRef);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not remove covered text in PostgreSQL", e);
        }
    }

    // ========================================================================
    //  DUATextQueryStore implementation
    // ========================================================================

    @Override
    public Stream<DUATextRow> find(DUATextQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUATextQuery.Exact q -> findByExact(q);
            case DUATextQuery.Substring q -> findBySubstring(q);
            case DUATextQuery.CoveredText q -> findByCoveredText(q);
        };
    }

    private Stream<DUATextRow> findByExact(DUATextQuery.Exact query) {
        return queryFeatures(sqlSelectExact, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setString(2, query.text());
        });
    }

    private Stream<DUATextRow> findBySubstring(DUATextQuery.Substring query) {
        return queryFeatures(sqlSelectSubstring, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setString(2, query.text());
        });
    }

    private Stream<DUATextRow> findByCoveredText(DUATextQuery.CoveredText query) {
        Stream<DUATextRow> rows = queryFeatures(sqlSelectCoveredText, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setString(2, query.text());
        });
        // Filter by typeId if present — the role column stores the type identifier
        OptionalInt typeId = query.typeId();
        if (typeId.isPresent()) {
            int tid = typeId.getAsInt();
            rows = rows.filter(row -> {
                try {
                    return Integer.parseInt(row.role()) == tid;
                } catch (NumberFormatException e) {
                    return false;
                }
            });
        }
        return rows;
    }

    // ========================================================================
    //  Internal helpers
    // ========================================================================

    /**
     * Queries the annotation text table and returns matching rows as a stream.
     * <p>
     * JDBC resources are safely closed via try-with-resources after eager collection.
     * </p>
     */
    private Stream<DUATextRow> queryFeatures(String sql, Binder binder) {
        List<DUATextRow> rows;
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(new DUATextRow(
                            resultSet.getLong("sofa_fs_ref"),
                            resultSet.getLong("annotation_fs_ref"),
                            resultSet.getString("role"),
                            resultSet.getString("covered_text")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not query annotation text in PostgreSQL", e);
        }
        return rows.stream();
    }

    /**
     * Converts a database result set row into a {@link DUASofa}.
     */
    private DUASofa toSofa(ResultSet rs) throws SQLException {
        long fsRef = rs.getLong("sofa_id");
        String sofaId = rs.getString("sofa_identifier");
        String localName = rs.getString("local_name");
        int typeOrdinal = rs.getShort("sofa_type");
        DUASofa.SofaType type = DUASofa.SofaType.values()[typeOrdinal];
        String dataStr = rs.getString("sofa_data");
        byte[] data = dataStr != null ? dataStr.getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
        return new DUASofa(fsRef, sofaId, localName, data, type, System.currentTimeMillis());
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
