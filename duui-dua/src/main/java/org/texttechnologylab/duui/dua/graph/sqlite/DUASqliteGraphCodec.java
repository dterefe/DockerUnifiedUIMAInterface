package org.texttechnologylab.duui.dua.graph.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;
import org.texttechnologylab.duui.dua.graph.DUAGraphEdge;
import org.texttechnologylab.duui.dua.graph.DUAGraphNode;
import org.texttechnologylab.duui.dua.graph.DUAGraphPartition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public final class DUASqliteGraphCodec implements DUAGraphCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String id() {
        return "sqlite";
    }

    @Override
    public String defaultFileName() {
        return "graph.sqlite";
    }

    @Override
    public void write(DUAGraphPartition partition, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Files.deleteIfExists(target);
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + target)) {
            try (var statement = connection.createStatement()) {
                statement.execute("PRAGMA foreign_keys = ON");
                statement.execute("CREATE TABLE nodes(id TEXT PRIMARY KEY, label TEXT NOT NULL, properties TEXT NOT NULL)");
                statement.execute("CREATE TABLE edges(id TEXT PRIMARY KEY, label TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL, properties TEXT NOT NULL)");
                statement.execute("CREATE INDEX edges_source_idx ON edges(source)");
                statement.execute("CREATE INDEX edges_target_idx ON edges(target)");
                statement.execute("CREATE INDEX nodes_label_idx ON nodes(label)");
            }
            connection.setAutoCommit(false);
            try (var node = connection.prepareStatement("INSERT INTO nodes(id, label, properties) VALUES (?, ?, ?)");
                 var edge = connection.prepareStatement("INSERT INTO edges(id, label, source, target, properties) VALUES (?, ?, ?, ?, ?)")) {
                for (DUAGraphNode graphNode : partition.nodes().toList()) {
                    node.setString(1, graphNode.id());
                    node.setString(2, graphNode.label());
                    node.setString(3, MAPPER.writeValueAsString(graphNode.properties()));
                    node.addBatch();
                }
                node.executeBatch();
                for (DUAGraphEdge graphEdge : partition.edges().toList()) {
                    edge.setString(1, graphEdge.id());
                    edge.setString(2, graphEdge.label());
                    edge.setString(3, graphEdge.source());
                    edge.setString(4, graphEdge.target());
                    edge.setString(5, MAPPER.writeValueAsString(graphEdge.properties()));
                    edge.addBatch();
                }
                edge.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            throw new IOException("Could not write DUA SQLite graph partition " + partition.id(), e);
        }
    }

    @Override
    public DUAGraphPartition read(String partitionId, String scope, Path source) throws IOException {
        DUAGraphPartition partition = new DUAGraphPartition(partitionId, scope);
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + source)) {
            try (var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT id, label, properties FROM nodes ORDER BY rowid")) {
                while (rows.next()) {
                    partition.node(new DUAGraphNode(
                            rows.getString("id"),
                            rows.getString("label"),
                            properties(rows.getString("properties"))
                    ));
                }
            }
            try (var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT id, label, source, target, properties FROM edges ORDER BY rowid")) {
                while (rows.next()) {
                    partition.edge(new DUAGraphEdge(
                            rows.getString("id"),
                            rows.getString("label"),
                            rows.getString("source"),
                            rows.getString("target"),
                            properties(rows.getString("properties"))
                    ));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Could not read DUA SQLite graph partition " + partitionId, e);
        }
        return partition;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(String json) throws IOException {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        return MAPPER.readValue(json, Map.class);
    }
}
