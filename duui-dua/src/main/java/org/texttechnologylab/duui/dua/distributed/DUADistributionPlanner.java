package org.texttechnologylab.duui.dua.distributed;

import java.util.ArrayList;
import java.util.List;

public final class DUADistributionPlanner {
    private DUADistributionPlanner() {
    }

    public static DUADistributionPlan rangePlan(String partitionId,
                                                long itemCount,
                                                int shardCount,
                                                int replicaCount,
                                                List<String> nodeUris) {
        if (itemCount < 0) {
            throw new IllegalArgumentException("itemCount must not be negative");
        }
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        if (replicaCount < 0) {
            throw new IllegalArgumentException("replicaCount must not be negative");
        }
        if (nodeUris == null || nodeUris.isEmpty()) {
            throw new IllegalArgumentException("nodeUris must not be empty");
        }
        int placementWidth = Math.min(nodeUris.size(), replicaCount + 1);
        List<DUAShardManifest> shards = new ArrayList<>(shardCount);
        List<DUAShardRoute> routes = new ArrayList<>(shardCount);
        long baseSize = itemCount / shardCount;
        long remainder = itemCount % shardCount;
        long cursor = 0;
        for (int shardIndex = 0; shardIndex < shardCount; shardIndex++) {
            long size = baseSize + (shardIndex < remainder ? 1 : 0);
            long start = cursor;
            long end = cursor + size;
            cursor = end;

            String shardId = partitionId + ".shard-" + shardIndex;
            List<DUAShardReplica> replicas = replicasForShard(nodeUris, shardIndex, placementWidth);
            String primaryUri = replicas.get(0).uri();
            List<String> replicaUris = replicas.stream().skip(1).map(DUAShardReplica::uri).toList();
            routes.add(new DUAShardRoute(partitionId, shardId, start, end, primaryUri, replicaUris));
            shards.add(new DUAShardManifest(shardId, partitionId, 0, start, end,
                    List.of(new DUAShardObjectRef("graphs/" + partitionId + "/" + shardId + ".dua-part",
                            "0000000000000000000000000000000000000000000000000000000000000000",
                            0)),
                    replicas));
        }
        return new DUADistributionPlan(partitionId, itemCount, shards,
                new DUARoutingTable(partitionId + ".routing", 0, routes));
    }

    private static List<DUAShardReplica> replicasForShard(List<String> nodeUris, int shardIndex, int placementWidth) {
        List<DUAShardReplica> replicas = new ArrayList<>(placementWidth);
        for (int replicaIndex = 0; replicaIndex < placementWidth; replicaIndex++) {
            int nodeIndex = (shardIndex + replicaIndex) % nodeUris.size();
            DUAShardReplica.Role role = replicaIndex == 0
                    ? DUAShardReplica.Role.LEADER
                    : DUAShardReplica.Role.FOLLOWER;
            replicas.add(new DUAShardReplica("node-" + nodeIndex, nodeUris.get(nodeIndex),
                    role, DUAShardReplica.State.ONLINE, 0));
        }
        return replicas;
    }
}
