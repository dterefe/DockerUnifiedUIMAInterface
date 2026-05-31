package org.texttechnologylab.duui.dua;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.dua.backend.DUABackendLayout;
import org.texttechnologylab.duui.dua.backend.DUAStoreRole;
import org.texttechnologylab.duui.dua.backend.postgres.DUAPostgresDeployment;
import org.texttechnologylab.duui.dua.backend.postgres.DUAPostgresRangeType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUAPostgresBackendTest {
    @Test
    void postgresLayoutDeclaresRangeAndAgeImplementations() {
        DUABackendLayout layout = DUABackendLayout.postgres();

        assertEquals("btree_gist", layout.store(DUAStoreRole.ANNOTATION_RANGE)
                .orElseThrow()
                .parameters()
                .get("extensions"));
        assertEquals("int4range,int8range", layout.store(DUAStoreRole.ANNOTATION_RANGE)
                .orElseThrow()
                .parameters()
                .get("rangeTypes"));
        assertEquals("age", layout.store(DUAStoreRole.TYPESYSTEM_GRAPH)
                .orElseThrow()
                .parameters()
                .get("extensions"));
    }

    @Test
    void postgresRangeTypeSelectsExplicitRangeConstructor() {
        assertEquals("int4range(?, ?, '[)')", DUAPostgresRangeType.INT4.expression());
        assertEquals("int8range(?, ?, '[)')", DUAPostgresRangeType.INT8.expression());
    }

    @Test
    void postgresDeploymentDescribesLocalPodmanDatabase() {
        DUAPostgresDeployment deployment = DUAPostgresDeployment.local().withHostPort(55433);

        assertEquals("docker.io/library/postgres:16", deployment.image());
        assertEquals("jdbc:postgresql://127.0.0.1:55433/dua", deployment.jdbcUrl());
        assertTrue(deployment.environmentList().contains("POSTGRES_DB=dua"));
    }
}
