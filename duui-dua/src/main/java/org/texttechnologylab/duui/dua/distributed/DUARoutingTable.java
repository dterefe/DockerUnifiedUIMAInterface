package org.texttechnologylab.duui.dua.distributed;

import java.util.Comparator;
import java.util.List;

public record DUARoutingTable(String schema,
                              String routingTableId,
                              long epoch,
                              List<DUAShardRoute> routes) {
    public static final String SCHEMA = "dua.distributed.routing.v1";

    public DUARoutingTable(String routingTableId, long epoch, List<DUAShardRoute> routes) {
        this(SCHEMA, routingTableId, epoch, routes);
    }

    public DUARoutingTable {
        if (!SCHEMA.equals(schema)) {
            throw new IllegalArgumentException("Unsupported routing table schema: " + schema);
        }
        if (routingTableId == null || routingTableId.isBlank()) {
            throw new IllegalArgumentException("routingTableId must not be blank");
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must not be negative");
        }
        routes = routes == null
                ? List.of()
                : routes.stream().sorted(Comparator.comparingLong(DUAShardRoute::rangeStart)).toList();
    }

    public DUAShardRoute routeForOrdinal(long ordinal) {
        int low = 0;
        int high = routes.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            DUAShardRoute route = routes.get(middle);
            if (ordinal < route.rangeStart()) {
                high = middle - 1;
            } else if (ordinal >= route.rangeEndExclusive()) {
                low = middle + 1;
            } else {
                return route;
            }
        }
        throw new IllegalArgumentException("No route for ordinal " + ordinal);
    }
}
