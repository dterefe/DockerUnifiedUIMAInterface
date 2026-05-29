package org.texttechnologylab.duui.dua;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUAQueryBackendSchemaTest {
    @Test
    void postgresQueryBackendSchemaIsPackagedAndTyped() throws Exception {
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream("dua/schema/postgres/query-backend-v1.sql")) {
            assertNotNull(input);
            String schema = new String(input.readAllBytes(), StandardCharsets.UTF_8);

            assertTrue(schema.contains("CREATE TABLE IF NOT EXISTS dua_q_feature_i64"));
            assertTrue(schema.contains("CREATE TABLE IF NOT EXISTS dua_q_feature_f64"));
            assertTrue(schema.contains("CREATE TABLE IF NOT EXISTS dua_q_feature_bool"));
            assertTrue(schema.contains("CREATE TABLE IF NOT EXISTS dua_q_feature_term"));
            assertTrue(schema.contains("CREATE TABLE IF NOT EXISTS dua_q_srl_event"));
            assertTrue(schema.contains("CREATE TABLE IF NOT EXISTS dua_q_association_edge"));
            assertTrue(schema.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS dua_q_timeline_event"));
        }
    }

    @Test
    void postgresQueryPatternsArePackagedAsCandidateSetPrimitives() throws Exception {
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream("dua/schema/postgres/query-patterns-v1.sql")) {
            assertNotNull(input);
            String schema = new String(input.readAllBytes(), StandardCharsets.UTF_8);

            assertTrue(schema.contains("CREATE OR REPLACE FUNCTION dua_q_scope_documents"));
            assertTrue(schema.contains("CREATE OR REPLACE FUNCTION dua_q_fulltext_candidates"));
            assertTrue(schema.contains("CREATE OR REPLACE FUNCTION dua_q_metadata_term_candidates"));
            assertTrue(schema.contains("CREATE OR REPLACE FUNCTION dua_q_srl_candidates"));
            assertTrue(schema.contains("CREATE OR REPLACE FUNCTION dua_q_association_neighbors"));
        }
    }
}
