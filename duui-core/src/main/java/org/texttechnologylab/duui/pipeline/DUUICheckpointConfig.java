package org.texttechnologylab.duui.pipeline;

public final class DUUICheckpointConfig {
    public static final DUUICheckpointConfig DEFAULT = new DUUICheckpointConfig(1, true, false);

    private final int maxInFlightArtifacts;
    private final boolean preserveOrder;
    private final boolean durable;

    public DUUICheckpointConfig(int maxInFlightArtifacts, boolean preserveOrder, boolean durable) {
        this.maxInFlightArtifacts = Math.max(1, maxInFlightArtifacts);
        this.preserveOrder = preserveOrder;
        this.durable = durable;
    }

    public int maxInFlightArtifacts() { return maxInFlightArtifacts; }
    public boolean preserveOrder() { return preserveOrder; }
    public boolean durable() { return durable; }
}
