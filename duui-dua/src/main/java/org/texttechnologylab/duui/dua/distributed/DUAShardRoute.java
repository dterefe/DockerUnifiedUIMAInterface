package org.texttechnologylab.duui.dua.distributed;

import java.util.List;

public record DUAShardRoute(String partitionId,
                            String shardId,
                            long rangeStart,
                            long rangeEndExclusive,
                            String primaryUri,
                            List<String> replicaUris) {
    public DUAShardRoute {
        if (partitionId == null || partitionId.isBlank()) {
            throw new IllegalArgumentException("partitionId must not be blank");
        }
        if (shardId == null || shardId.isBlank()) {
            throw new IllegalArgumentException("shardId must not be blank");
        }
        if (rangeStart < 0 || rangeEndExclusive < rangeStart) {
            throw new IllegalArgumentException("invalid shard range");
        }
        if (primaryUri == null || primaryUri.isBlank()) {
            throw new IllegalArgumentException("primaryUri must not be blank");
        }
        replicaUris = replicaUris == null ? List.of() : List.copyOf(replicaUris);
    }

    public boolean contains(long ordinal) {
        return ordinal >= rangeStart && ordinal < rangeEndExclusive;
    }
}
