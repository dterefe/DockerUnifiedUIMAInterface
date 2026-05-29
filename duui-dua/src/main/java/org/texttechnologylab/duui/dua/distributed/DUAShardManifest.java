package org.texttechnologylab.duui.dua.distributed;

import java.util.List;

public record DUAShardManifest(String schema,
                               String shardId,
                               String partitionId,
                               long epoch,
                               long rangeStart,
                               long rangeEndExclusive,
                               List<DUAShardObjectRef> objects,
                               List<DUAShardReplica> replicas) {
    public static final String SCHEMA = "dua.distributed.shard.v1";

    public DUAShardManifest(String shardId,
                            String partitionId,
                            long epoch,
                            long rangeStart,
                            long rangeEndExclusive,
                            List<DUAShardObjectRef> objects,
                            List<DUAShardReplica> replicas) {
        this(SCHEMA, shardId, partitionId, epoch, rangeStart, rangeEndExclusive, objects, replicas);
    }

    public DUAShardManifest {
        if (!SCHEMA.equals(schema)) {
            throw new IllegalArgumentException("Unsupported shard manifest schema: " + schema);
        }
        if (shardId == null || shardId.isBlank()) {
            throw new IllegalArgumentException("shardId must not be blank");
        }
        if (partitionId == null || partitionId.isBlank()) {
            throw new IllegalArgumentException("partitionId must not be blank");
        }
        if (epoch < 0 || rangeStart < 0 || rangeEndExclusive < rangeStart) {
            throw new IllegalArgumentException("invalid shard manifest range or epoch");
        }
        objects = objects == null ? List.of() : List.copyOf(objects);
        replicas = replicas == null ? List.of() : List.copyOf(replicas);
    }
}
