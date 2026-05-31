package org.texttechnologylab.duui.dua;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.backend.DUABackendLayout;
import org.texttechnologylab.duui.dua.backend.DUAStoreRole;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUAArchiveTest {
    @TempDir
    Path temp;

    @Test
    void writesAndReadsArchiveManifestWithSemanticBackendLayout() throws Exception {
        Path archive = temp.resolve("sample.dua");

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-1")) {
            writer.addArtifact("doc-1", "text", "text/plain", "hello".getBytes(StandardCharsets.UTF_8));
            writer.addStoreSnapshot(DUAStoreRole.TEXT_SEARCH, "text-index", "application/json",
                    "{\"segments\":[]}".getBytes(StandardCharsets.UTF_8));
        }

        assertTrue(Files.exists(archive));
        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            assertEquals("DUA", reader.manifest().getFormat());
            assertEquals("universe-1", reader.manifest().getUniverseId());
            assertEquals(1, reader.manifest().getArtifacts().size());
            assertTrue(reader.manifest().getBackendLayout().store(DUAStoreRole.RELATIONAL_VALUE).isPresent());
            assertTrue(reader.manifest().getBackendLayout().store(DUAStoreRole.ANNOTATION_RANGE).isPresent());
            assertTrue(reader.manifest().getBackendLayout().store(DUAStoreRole.TYPESYSTEM_GRAPH).isPresent());
            assertTrue(reader.manifest().getBackendLayout().store(DUAStoreRole.TEXT_SEARCH).isPresent());
            assertEquals("hello", new String(reader.artifactPayload("doc-1"), StandardCharsets.UTF_8));
            assertEquals("{\"segments\":[]}", new String(reader.storeSnapshotPayload(
                    DUAStoreRole.TEXT_SEARCH, "text-index"), StandardCharsets.UTF_8));
        }
    }

    @Test
    void archiveManifestCanDeclarePostgresSemanticBackendLayout() throws Exception {
        Path archive = temp.resolve("postgres.dua");

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-1")) {
            writer.backendLayout(DUABackendLayout.postgres());
            writer.addStoreSnapshot(DUAStoreRole.RELATIONAL_VALUE, "cas-values", "application/json",
                    "{\"rows\":[]}".getBytes(StandardCharsets.UTF_8));
            writer.addArtifact("doc-1", "text", "text/plain", "hello".getBytes(StandardCharsets.UTF_8));
        }

        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            assertEquals("duui-dua-postgres", reader.manifest().getBackendLayout().getId());
            assertEquals(1, reader.manifest().getStoreSnapshots().size());
            assertEquals("{\"rows\":[]}", new String(reader.storeSnapshotPayload(
                    DUAStoreRole.RELATIONAL_VALUE, "cas-values"), StandardCharsets.UTF_8));
            assertEquals("postgresql-range-gist", reader.manifest().getBackendLayout()
                    .store(DUAStoreRole.ANNOTATION_RANGE)
                    .orElseThrow()
                    .implementation());
            assertEquals("pg_trgm", reader.manifest().getBackendLayout()
                    .store(DUAStoreRole.TEXT_SEARCH)
                    .orElseThrow()
                    .parameters()
                    .get("extensions"));
        }
    }

    @Test
    void archiveLayoutContainsOnlyResetPayloadDirectories() throws Exception {
        Path archive = temp.resolve("sample.dua");

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-1")) {
            writer.addArtifact("doc-1", "text", "text/plain", "hello".getBytes(StandardCharsets.UTF_8));
            writer.addStoreSnapshot(DUAStoreRole.ANNOTATION_RANGE, "span-index", "application/json",
                    "{\"spans\":[]}".getBytes(StandardCharsets.UTF_8));
        }

        try (ZipFile zip = new ZipFile(archive.toFile())) {
            assertFalse(zip.stream().anyMatch(entry -> entry.getName().startsWith("graphs/")));
            assertFalse(zip.stream().anyMatch(entry -> entry.getName().equals("indexes/virtual_corpora.json")));
            assertTrue(zip.stream().anyMatch(entry -> entry.getName().startsWith("stores/")));
        }
    }
}
