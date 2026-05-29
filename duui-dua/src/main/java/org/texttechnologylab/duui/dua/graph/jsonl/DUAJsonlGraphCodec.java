package org.texttechnologylab.duui.dua.graph.jsonl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;
import org.texttechnologylab.duui.dua.graph.DUAGraphEdge;
import org.texttechnologylab.duui.dua.graph.DUAGraphNode;
import org.texttechnologylab.duui.dua.graph.DUAGraphPartition;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class DUAJsonlGraphCodec implements DUAGraphCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String id() {
        return "jsonl";
    }

    @Override
    public String defaultFileName() {
        return "graph.jsonl";
    }

    @Override
    public void write(DUAGraphPartition partition, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(target)) {
            for (DUAGraphNode node : partition.nodes().toList()) {
                writer.write(MAPPER.writeValueAsString(Map.of(
                        "kind", "node",
                        "id", node.id(),
                        "label", node.label(),
                        "properties", node.properties()
                )));
                writer.newLine();
            }
            for (DUAGraphEdge edge : partition.edges().toList()) {
                writer.write(MAPPER.writeValueAsString(Map.of(
                        "kind", "edge",
                        "id", edge.id(),
                        "label", edge.label(),
                        "source", edge.source(),
                        "target", edge.target(),
                        "properties", edge.properties()
                )));
                writer.newLine();
            }
        }
    }

    @Override
    public DUAGraphPartition read(String partitionId, String scope, Path source) throws IOException {
        DUAGraphPartition partition = new DUAGraphPartition(partitionId, scope);
        try (BufferedReader reader = Files.newBufferedReader(source)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                Map<?, ?> record = MAPPER.readValue(line, Map.class);
                String kind = string(record.get("kind"));
                if ("node".equals(kind)) {
                    partition.node(new DUAGraphNode(
                            string(record.get("id")),
                            string(record.get("label")),
                            properties(record.get("properties"))
                    ));
                } else if ("edge".equals(kind)) {
                    partition.edge(new DUAGraphEdge(
                            string(record.get("id")),
                            string(record.get("label")),
                            string(record.get("source")),
                            string(record.get("target")),
                            properties(record.get("properties"))
                    ));
                }
            }
        }
        return partition;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Object value) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Map.of();
    }

    private static String string(Object value) {
        return value == null ? "" : value.toString();
    }
}
