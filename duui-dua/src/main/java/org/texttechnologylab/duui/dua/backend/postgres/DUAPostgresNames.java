package org.texttechnologylab.duui.dua.backend.postgres;

import java.util.Objects;
import java.util.regex.Pattern;

final class DUAPostgresNames {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private DUAPostgresNames() {
    }

    static String relation(String relation) {
        Objects.requireNonNull(relation, "relation");
        String[] parts = relation.split("\\.", -1);
        if (parts.length < 1 || parts.length > 2) {
            throw new IllegalArgumentException("PostgreSQL relation must be table or schema.table: " + relation);
        }
        for (String part : parts) {
            if (!IDENTIFIER.matcher(part).matches()) {
                throw new IllegalArgumentException("Invalid PostgreSQL identifier in relation: " + relation);
            }
        }
        return relation;
    }

    static String graph(String graph) {
        Objects.requireNonNull(graph, "graph");
        if (!IDENTIFIER.matcher(graph).matches()) {
            throw new IllegalArgumentException("Invalid AGE graph name: " + graph);
        }
        return graph;
    }

    static String literal(String value) {
        Objects.requireNonNull(value, "value");
        return "'" + value.replace("'", "''") + "'";
    }
}
