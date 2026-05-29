package org.texttechnologylab.duui.dua.distributed;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

public record DUAShardReplica(String nodeId,
                              String uri,
                              Role role,
                              State state,
                              long lastAppliedSequence) {
    public enum Role {
        LEADER,
        FOLLOWER,
        LEARNER,
        CACHE;

        @JsonValue
        public String jsonValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum State {
        ONLINE,
        SYNCING,
        STALE,
        OFFLINE;

        @JsonValue
        public String jsonValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public DUAShardReplica {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("uri must not be blank");
        }
        if (lastAppliedSequence < 0) {
            throw new IllegalArgumentException("lastAppliedSequence must not be negative");
        }
    }
}
