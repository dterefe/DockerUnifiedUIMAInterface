package org.texttechnologylab.duui.dua.distributed;

import java.util.List;

public record DUADistributionPlan(String partitionId,
                                  long itemCount,
                                  List<DUAShardManifest> shards,
                                  DUARoutingTable routingTable) {
    public DUADistributionPlan {
        if (partitionId == null || partitionId.isBlank()) {
            throw new IllegalArgumentException("partitionId must not be blank");
        }
        if (itemCount < 0) {
            throw new IllegalArgumentException("itemCount must not be negative");
        }
        shards = shards == null ? List.of() : List.copyOf(shards);
    }

    public DUAShardRoute routeForOrdinal(long ordinal) {
        return routingTable.routeForOrdinal(ordinal);
    }
}
