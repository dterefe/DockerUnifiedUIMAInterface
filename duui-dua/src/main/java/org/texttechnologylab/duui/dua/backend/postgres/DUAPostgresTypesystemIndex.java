package org.texttechnologylab.duui.dua.backend.postgres;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUATypeQuery;
import org.texttechnologylab.duui.dua.store.DUARevision;
import org.texttechnologylab.duui.dua.store.DUATypeNode;
import org.texttechnologylab.duui.dua.store.DUATypesystemIndex;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class DUAPostgresTypesystemIndex implements DUATypesystemIndex {
    public static final String DEFAULT_GRAPH = "dua_typesystem";
    public static final String DEFAULT_TYPE_TABLE = "dua_types";

    private static final Pattern AGTYPE_INTEGER = Pattern.compile("-?\\d+");

    private final DUAPostgresConnectionProvider connections;
    private final String graph;
    private final String typeTable;

    public DUAPostgresTypesystemIndex(DUAPostgresConnectionProvider connections) {
        this(connections, DEFAULT_GRAPH, DEFAULT_TYPE_TABLE);
    }

    public DUAPostgresTypesystemIndex(DUAPostgresConnectionProvider connections, String graph, String typeTable) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.graph = DUAPostgresNames.graph(graph);
        this.typeTable = DUAPostgresNames.relation(typeTable);
    }

    public void initializeAgeGraph() {
        try (Connection connection = connections.openConnection()) {
            prepareAge(connection);
            if (!ageGraphExists(connection)) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("select create_graph(" + DUAPostgresNames.literal(graph) + ")");
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not initialize DUA PostgreSQL AGE graph", e);
        }
    }

    @Override
    public DUAWriteResult index(DUATypeNode node) {
        Objects.requireNonNull(node, "node");
        try (Connection connection = connections.openConnection()) {
            upsertMirror(connection, node);
            prepareAge(connection);
            mergeTypeNode(connection, node);
            if (node.parentTypeId().isPresent()) {
                mergeSubtypeEdge(connection, node.typeId(), node.parentTypeId().getAsInt());
            }
            return new DUAWriteResult(DUAId.of("type-" + node.typeId()), new DUARevision(1));
        } catch (SQLException e) {
            throw new IllegalStateException("Could not index DUA typesystem node in PostgreSQL AGE", e);
        }
    }

    public void indexReference(String sourceTypeName, String featureName, String targetTypeName) {
        Objects.requireNonNull(sourceTypeName, "sourceTypeName");
        Objects.requireNonNull(featureName, "featureName");
        Objects.requireNonNull(targetTypeName, "targetTypeName");
        String cypher = "MATCH (source:Type {type_name: " + cypherString(sourceTypeName) + "}) "
                + "MATCH (target:Type {type_name: " + cypherString(targetTypeName) + "}) "
                + "MERGE (source)-[r:FEATURE_REF {feature_name: " + cypherString(featureName) + "}]->(target) "
                + "RETURN count(r)";
        try (Connection connection = connections.openConnection()) {
            prepareAge(connection);
            executeCypher(connection, cypher, "count agtype");
        } catch (SQLException e) {
            throw new IllegalStateException("Could not index DUA typesystem feature reference in PostgreSQL AGE", e);
        }
    }

    @Override
    public Stream<DUATypeNode> find(DUATypeQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUATypeQuery.ExactType q -> exact(q.typeName()).stream();
            case DUATypeQuery.Subtypes q -> byTypeIds(ageTypeIds(subtypeCypher(q.typeName(), q.transitive()))).stream();
            case DUATypeQuery.Supertypes q -> byTypeIds(ageTypeIds(supertypeCypher(q.typeName(), q.transitive()))).stream();
            // Instance-level reference traversal is not supported via PostgreSQL AGE;
            // the in-memory index handles these queries through reverse/forward reference maps.
            case DUATypeQuery.ReferenceTraversal q -> Stream.empty();
            case DUATypeQuery.OutgoingReferences q -> Stream.empty();
        };
    }

    private List<DUATypeNode> exact(String typeName) {
        String sql = "select type_id, type_name, parent_type_id from " + typeTable + " where type_name = ?";
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, typeName);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<DUATypeNode> nodes = new ArrayList<>();
                while (resultSet.next()) {
                    nodes.add(typeNode(resultSet));
                }
                return nodes;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not query DUA typesystem mirror table", e);
        }
    }

    private List<Integer> ageTypeIds(String cypher) {
        try (Connection connection = connections.openConnection()) {
            prepareAge(connection);
            try (ResultSet resultSet = executeCypher(connection, cypher, "type_id agtype")) {
                Set<Integer> ids = new LinkedHashSet<>();
                while (resultSet.next()) {
                    parseAgtypeInteger(resultSet.getObject(1)).ifPresent(ids::add);
                }
                return List.copyOf(ids);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not query DUA typesystem AGE graph", e);
        }
    }

    private List<DUATypeNode> byTypeIds(List<Integer> typeIds) {
        if (typeIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", typeIds.stream().map(id -> "?").toList());
        String sql = "select type_id, type_name, parent_type_id from " + typeTable
                + " where type_id in (" + placeholders + ") order by type_id";
        try (Connection connection = connections.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < typeIds.size(); i++) {
                statement.setInt(i + 1, typeIds.get(i));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                List<DUATypeNode> nodes = new ArrayList<>();
                while (resultSet.next()) {
                    nodes.add(typeNode(resultSet));
                }
                return nodes;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not fetch DUA typesystem mirror rows", e);
        }
    }

    private void upsertMirror(Connection connection, DUATypeNode node) throws SQLException {
        String sql = "insert into " + typeTable + " (type_id, type_name, parent_type_id) values (?, ?, ?) "
                + "on conflict (type_id) do update set "
                + "type_name = excluded.type_name, parent_type_id = excluded.parent_type_id";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, node.typeId());
            statement.setString(2, node.typeName());
            if (node.parentTypeId().isPresent()) {
                statement.setInt(3, node.parentTypeId().getAsInt());
            } else {
                statement.setNull(3, Types.INTEGER);
            }
            statement.executeUpdate();
        }
    }

    private void mergeTypeNode(Connection connection, DUATypeNode node) throws SQLException {
        String cypher = "MERGE (type:Type {type_id: " + node.typeId() + "}) "
                + "SET type.type_name = " + cypherString(node.typeName()) + " "
                + "RETURN type.type_id";
        executeCypher(connection, cypher, "type_id agtype").close();
    }

    private void mergeSubtypeEdge(Connection connection, int childTypeId, int parentTypeId) throws SQLException {
        String cypher = "MERGE (child:Type {type_id: " + childTypeId + "}) "
                + "MERGE (parent:Type {type_id: " + parentTypeId + "}) "
                + "MERGE (child)-[rel:SUBTYPE_OF]->(parent) "
                + "RETURN child.type_id";
        executeCypher(connection, cypher, "type_id agtype").close();
    }

    private String subtypeCypher(String typeName, boolean transitive) {
        String edge = transitive ? "[:SUBTYPE_OF*1..]" : "[:SUBTYPE_OF]";
        return "MATCH (root:Type {type_name: " + cypherString(typeName) + "})<-"
                + edge + "-(child:Type) RETURN child.type_id";
    }

    private String supertypeCypher(String typeName, boolean transitive) {
        String edge = transitive ? "[:SUBTYPE_OF*1..]" : "[:SUBTYPE_OF]";
        return "MATCH (leaf:Type {type_name: " + cypherString(typeName) + "})-"
                + edge + "->(parent:Type) RETURN parent.type_id";
    }

    private ResultSet executeCypher(Connection connection, String cypher, String columns) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery("select * from cypher("
                + DUAPostgresNames.literal(graph) + ", $$ " + cypher + " $$) as (" + columns + ")");
    }

    private void prepareAge(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("load 'age'");
            statement.execute("set search_path = ag_catalog, \"$user\", public");
        }
    }

    private boolean ageGraphExists(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select 1 from ag_catalog.ag_graph where name = ?")) {
            statement.setString(1, graph);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private DUATypeNode typeNode(ResultSet resultSet) throws SQLException {
        int parent = resultSet.getInt("parent_type_id");
        return new DUATypeNode(
                resultSet.getInt("type_id"),
                resultSet.getString("type_name"),
                resultSet.wasNull() ? OptionalInt.empty() : OptionalInt.of(parent));
    }

    private OptionalInt parseAgtypeInteger(Object value) {
        if (value == null) {
            return OptionalInt.empty();
        }
        Matcher matcher = AGTYPE_INTEGER.matcher(value.toString());
        if (!matcher.find()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Integer.parseInt(matcher.group()));
    }

    private static String cypherString(String value) {
        Objects.requireNonNull(value, "value");
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
