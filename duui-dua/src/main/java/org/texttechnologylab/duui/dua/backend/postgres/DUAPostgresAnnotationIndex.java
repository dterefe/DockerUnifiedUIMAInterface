package org.texttechnologylab.duui.dua.backend.postgres;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;
import org.texttechnologylab.duui.dua.store.DUAAnnotationIndex;
import org.texttechnologylab.duui.dua.store.DUARevision;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

public final class DUAPostgresAnnotationIndex implements DUAAnnotationIndex {
    public static final String DEFAULT_TABLE = "dua_annotation_spans";

    private final DUAPostgresConnectionProvider connections;
    private final String table;
    private final DUAPostgresRangeType rangeType;

    public DUAPostgresAnnotationIndex(DUAPostgresConnectionProvider connections) {
        this(connections, DEFAULT_TABLE, DUAPostgresRangeType.INT4);
    }

    public DUAPostgresAnnotationIndex(DUAPostgresConnectionProvider connections,
                                      String table,
                                      DUAPostgresRangeType rangeType) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.table = DUAPostgresNames.relation(table);
        this.rangeType = rangeType == null ? DUAPostgresRangeType.INT4 : rangeType;
    }

    @Override
    public DUAWriteResult index(DUAAnnotationSpan span) {
        Objects.requireNonNull(span, "span");
        String sql = """
                insert into %s (sofa_fs_ref, fs_ref, type_id, begin_offset, end_offset, covered_text)
                values (?, ?, ?, ?, ?, ?)
                on conflict (fs_ref) do update set
                    sofa_fs_ref = excluded.sofa_fs_ref,
                    type_id = excluded.type_id,
                    begin_offset = excluded.begin_offset,
                    end_offset = excluded.end_offset,
                    covered_text = excluded.covered_text
                """.formatted(table);
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, span.sofaFsRef());
            statement.setLong(2, span.fsRef());
            statement.setInt(3, span.typeId());
            statement.setInt(4, span.begin());
            statement.setInt(5, span.end());
            if (span.coveredText().isPresent()) {
                statement.setString(6, span.coveredText().get());
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            statement.executeUpdate();
            return new DUAWriteResult(DUAId.of("sofa-" + span.sofaFsRef() + "#ann-" + span.fsRef()),
                    new DUARevision(1));
        } catch (SQLException e) {
            throw new IllegalStateException("Could not index DUA annotation span in PostgreSQL", e);
        }
    }

    @Override
    public Stream<DUAAnnotationSpan> find(DUAAnnotationSpanQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUAAnnotationSpanQuery.ExactSpan q -> exact(q).stream();
            case DUAAnnotationSpanQuery.CoveringPoint q -> coveringPoint(q).stream();
            case DUAAnnotationSpanQuery.Overlapping q -> rangeQuery(q.sofaFsRef(), q.begin(), q.end(), q.typeId(), "span && " + range()).stream();
            case DUAAnnotationSpanQuery.ContainedIn q -> rangeQuery(q.sofaFsRef(), q.begin(), q.end(), q.typeId(), "span <@ " + range()).stream();
            case DUAAnnotationSpanQuery.CoveringSpan q -> rangeQuery(q.sofaFsRef(), q.begin(), q.end(), q.typeId(), "span @> " + range()).stream();
            case DUAAnnotationSpanQuery.Neighborhood q -> neighborhood(q).stream();
            case DUAAnnotationSpanQuery.SameSpanJoin q -> sameSpanJoin(q).stream();
            case DUAAnnotationSpanQuery.RangeJoin q -> rangeJoin(q).stream();
            case DUAAnnotationSpanQuery.CoveredText q -> text(q.sofaFsRef(), q.text(), q.typeId(), false).stream();
            case DUAAnnotationSpanQuery.Substring q -> text(q.sofaFsRef(), q.text(), q.typeId(), true).stream();
            case DUAAnnotationSpanQuery.Pattern q -> q.steps().stream().flatMap(this::find).distinct();
        };
    }

    private List<DUAAnnotationSpan> exact(DUAAnnotationSpanQuery.ExactSpan query) {
        String sql = "select * from " + table
                + " where sofa_fs_ref = ? and begin_offset = ? and end_offset = ? and (? is null or type_id = ?)"
                + " order by begin_offset, end_offset, fs_ref";
        return select(sql, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setInt(2, query.begin());
            statement.setInt(3, query.end());
            bindOptionalType(statement, 4, query.typeId());
        });
    }

    private List<DUAAnnotationSpan> coveringPoint(DUAAnnotationSpanQuery.CoveringPoint query) {
        String sql = "select * from " + table
                + " where sofa_fs_ref = ? and span @> ? and (? is null or type_id = ?)"
                + " order by begin_offset, end_offset, fs_ref";
        return select(sql, statement -> {
            statement.setLong(1, query.sofaFsRef());
            rangeType.bindPoint(statement, 2, query.offset());
            bindOptionalType(statement, 3, query.typeId());
        });
    }

    private List<DUAAnnotationSpan> rangeQuery(long sofaFsRef, int begin, int end, OptionalInt typeId, String predicate) {
        String sql = "select * from " + table
                + " where sofa_fs_ref = ? and " + predicate + " and (? is null or type_id = ?)"
                + " order by begin_offset, end_offset, fs_ref";
        return select(sql, statement -> {
            statement.setLong(1, sofaFsRef);
            rangeType.bindRange(statement, 2, begin, end);
            bindOptionalType(statement, 4, typeId);
        });
    }

    private List<DUAAnnotationSpan> neighborhood(DUAAnnotationSpanQuery.Neighborhood query) {
        String sql = """
                with ordered as (
                    select *, row_number() over (order by begin_offset, end_offset, fs_ref) as rn
                      from %s
                     where sofa_fs_ref = ?
                ),
                anchor as (
                    select rn from ordered where fs_ref = ?
                )
                select ordered.*
                  from ordered, anchor
                 where ordered.rn between anchor.rn - ? and anchor.rn + ?
                   and (? is null or ordered.type_id = ?)
                 order by ordered.rn
                """.formatted(table);
        return select(sql, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setLong(2, query.anchorFsRef());
            statement.setInt(3, query.before());
            statement.setInt(4, query.after());
            bindOptionalType(statement, 5, query.typeId());
        });
    }

    private List<DUAAnnotationSpan> sameSpanJoin(DUAAnnotationSpanQuery.SameSpanJoin query) {
        String sql = """
                select left_span.*
                  from %s left_span
                 where left_span.sofa_fs_ref = ?
                   and left_span.type_id = ?
                   and exists (
                       select 1
                         from %s right_span
                        where right_span.sofa_fs_ref = left_span.sofa_fs_ref
                          and right_span.type_id = ?
                          and right_span.begin_offset = left_span.begin_offset
                          and right_span.end_offset = left_span.end_offset
                   )
                 order by left_span.begin_offset, left_span.end_offset, left_span.fs_ref
                """.formatted(table, table);
        return select(sql, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setInt(2, query.leftTypeId());
            statement.setInt(3, query.rightTypeId());
        });
    }

    private List<DUAAnnotationSpan> rangeJoin(DUAAnnotationSpanQuery.RangeJoin query) {
        String sql = """
                select outer_span.*
                  from %s outer_span
                 where outer_span.sofa_fs_ref = ?
                   and outer_span.type_id = ?
                   and exists (
                       select 1
                         from %s inner_span
                        where inner_span.sofa_fs_ref = outer_span.sofa_fs_ref
                          and inner_span.type_id = ?
                          and inner_span.span <@ outer_span.span
                   )
                 order by outer_span.begin_offset, outer_span.end_offset, outer_span.fs_ref
                """.formatted(table, table);
        return select(sql, statement -> {
            statement.setLong(1, query.sofaFsRef());
            statement.setInt(2, query.outerTypeId());
            statement.setInt(3, query.innerTypeId());
        });
    }

    private List<DUAAnnotationSpan> text(long sofaFsRef, String text, OptionalInt typeId, boolean substring) {
        String predicate = substring ? "position(? in covered_text) > 0" : "covered_text = ?";
        String sql = "select * from " + table
                + " where sofa_fs_ref = ? and covered_text is not null and " + predicate
                + " and (? is null or type_id = ?) order by begin_offset, end_offset, fs_ref";
        return select(sql, statement -> {
            statement.setLong(1, sofaFsRef);
            statement.setString(2, text);
            bindOptionalType(statement, 3, typeId);
        });
    }

    private List<DUAAnnotationSpan> select(String sql, Binder binder) {
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<DUAAnnotationSpan> rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(row(resultSet));
                }
                return rows;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not query DUA annotation range index in PostgreSQL", e);
        }
    }

    private DUAAnnotationSpan row(ResultSet resultSet) throws SQLException {
        return new DUAAnnotationSpan(
                resultSet.getLong("sofa_fs_ref"),
                resultSet.getLong("fs_ref"),
                resultSet.getInt("type_id"),
                resultSet.getInt("begin_offset"),
                resultSet.getInt("end_offset"),
                Optional.ofNullable(resultSet.getString("covered_text")));
    }

    private String range() {
        return rangeType.expression();
    }

    private static void bindOptionalType(PreparedStatement statement, int parameter, OptionalInt typeId)
            throws SQLException {
        if (typeId.isPresent()) {
            statement.setInt(parameter, typeId.getAsInt());
            statement.setInt(parameter + 1, typeId.getAsInt());
        } else {
            statement.setNull(parameter, Types.INTEGER);
            statement.setNull(parameter + 1, Types.INTEGER);
        }
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
