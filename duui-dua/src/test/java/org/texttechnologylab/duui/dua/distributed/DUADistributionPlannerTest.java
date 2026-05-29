package org.texttechnologylab.duui.dua.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class DUADistributionPlannerTest {
    @Test
    void rangePlanRoutesCorpusOrdinalsToStableShardReplicas() {
        DUADistributionPlan plan = DUADistributionPlanner.rangePlan("corpus-news", 10, 3, 1,
                List.of("dua://node-a", "dua://node-b", "dua://node-c"));

        assertEquals(3, plan.shards().size());
        assertEquals("corpus-news.shard-0", plan.routeForOrdinal(0).shardId());
        assertEquals("corpus-news.shard-0", plan.routeForOrdinal(3).shardId());
        assertEquals("corpus-news.shard-1", plan.routeForOrdinal(4).shardId());
        assertEquals("corpus-news.shard-2", plan.routeForOrdinal(9).shardId());
        assertEquals("dua://node-a", plan.shards().get(0).replicas().get(0).uri());
        assertEquals("dua://node-b", plan.shards().get(0).replicas().get(1).uri());
        assertThrows(IllegalArgumentException.class, () -> plan.routeForOrdinal(10));
    }
}
