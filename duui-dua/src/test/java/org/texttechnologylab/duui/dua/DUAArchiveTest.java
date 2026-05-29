package org.texttechnologylab.duui.dua;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.distributed.DUADistributionPlan;
import org.texttechnologylab.duui.dua.distributed.DUADistributionPlanner;
import org.texttechnologylab.duui.dua.graph.DUAGraphEdge;
import org.texttechnologylab.duui.dua.graph.DUAGraphNode;
import org.texttechnologylab.duui.dua.graph.DUAGraphPartition;
import org.texttechnologylab.duui.dua.graph.jsonl.DUAJsonlGraphCodec;
import org.texttechnologylab.duui.dua.graph.sqlite.DUASqliteGraphCodec;
import org.texttechnologylab.duui.dua.uima.storage.DUAOrderedKvCasStorage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUAArchiveTest {
    @TempDir
    Path temp;

    @Test
    void writesAndReadsArchiveManifestAndJsonlPartition() throws Exception {
        Path archive = temp.resolve("sample.dua");
        DUAGraphPartition partition = samplePartition();

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-1")) {
            writer.addArtifact("doc-1", "text", "text/plain", "hello".getBytes());
            writer.addPartition(partition, new DUAJsonlGraphCodec());
        }

        assertTrue(Files.exists(archive));
        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            assertEquals("DUA", reader.manifest().getFormat());
            assertEquals("universe-1", reader.manifest().getUniverseId());
            assertEquals(1, reader.manifest().getArtifacts().size());
            assertEquals(1, reader.manifest().getPartitions().size());
            DUAGraphPartition restored = reader.partition("partition-1", "jsonl").orElseThrow();
            assertEquals(2, restored.nodeCount());
            assertEquals(1, restored.edgeCount());
            assertEquals("hello", new String(reader.artifactPayload("doc-1")));
        }
    }

    @Test
    void graphCodecsRoundTripEquivalentCounts() throws Exception {
        DUAGraphPartition partition = samplePartition();
        Path jsonl = temp.resolve("graph.jsonl");
        Path sqlite = temp.resolve("graph.sqlite");

        DUAJsonlGraphCodec jsonlCodec = new DUAJsonlGraphCodec();
        DUASqliteGraphCodec sqliteCodec = new DUASqliteGraphCodec();
        jsonlCodec.write(partition, jsonl);
        sqliteCodec.write(partition, sqlite);

        DUAGraphPartition fromJsonl = jsonlCodec.read("partition-1", "test", jsonl);
        DUAGraphPartition fromSqlite = sqliteCodec.read("partition-1", "test", sqlite);

        assertEquals(fromJsonl.nodeCount(), fromSqlite.nodeCount());
        assertEquals(fromJsonl.edgeCount(), fromSqlite.edgeCount());
    }

    @Test
    void writesDistributedRoutingAndShardManifestsIntoArchiveLayout() throws Exception {
        Path archive = temp.resolve("distributed.dua");
        DUADistributionPlan plan = DUADistributionPlanner.rangePlan("corpus-news", 8, 2, 1,
                List.of("dua://node-a", "dua://node-b"));

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-distributed")) {
            writer.addDistributionPlan(plan);
        }

        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            String routing = new String(reader.resourcePayload("distribution/routing/corpus-news.routing.json"));
            String shard = new String(reader.resourcePayload(
                    "partitions/corpus-news/shards/corpus-news.shard-0/manifest.json"));
            assertTrue(routing.contains("\"schema\" : \"dua.distributed.routing.v1\""));
            assertTrue(routing.contains("\"primaryUri\" : \"dua://node-a\""));
            assertTrue(shard.contains("\"schema\" : \"dua.distributed.shard.v1\""));
            assertTrue(shard.contains("\"role\" : \"leader\""));
        }
    }

    @Test
    void packsOrderedKvCasShardAsDistributedArchiveObjects() throws Exception {
        Path storageDirectory = temp.resolve("cas-shard-store");
        try (DUAOrderedKvCasStorage storage = new DUAOrderedKvCasStorage(storageDirectory)) {
            storage.writeIntSlot(1, 101, "begin", 42);
            storage.writeSlot(1, 102, "target", org.texttechnologylab.duui.dua.uima.storage.DUACasValue.ref(99));
        }

        Path archive = temp.resolve("cas-shard.dua");
        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-cas-shard")) {
            writer.addCasShardDirectory("cas-corpus-news", "cas-corpus-news.shard-0",
                    storageDirectory, 0, 10_000, List.of());
        }

        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            String manifest = new String(reader.resourcePayload(
                    "partitions/cas-corpus-news/shards/cas-corpus-news.shard-0/manifest.json"));
            byte[] wal = reader.resourcePayload(
                    "partitions/cas-corpus-news/shards/cas-corpus-news.shard-0/objects/cas-kv-v1.wal");
            assertTrue(manifest.contains("\"schema\" : \"dua.distributed.shard.v1\""));
            assertTrue(manifest.contains("\"objects\""));
            assertTrue(manifest.contains("\"sha256\""));
            assertTrue(wal.length > 0);
        }
    }

    private static DUAGraphPartition samplePartition() {
        return new DUAGraphPartition("partition-1", "test")
                .node(new DUAGraphNode("a", "artifact", Map.of("kind", "corpus")))
                .node(new DUAGraphNode("b", "artifact", Map.of("kind", "document")))
                .edge(new DUAGraphEdge("e", "contains", "a", "b", Map.of()));
    }
}
