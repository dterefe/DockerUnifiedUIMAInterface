package org.texttechnologylab.duui.dua.backend;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public record DUABackendStore(
        String id,
        DUAStoreRole role,
        String implementation,
        Set<DUAStoreCapability> capabilities,
        Map<String, String> parameters
) {
    public DUABackendStore {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(implementation, "implementation");
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
    }

    public static Builder builder(String id, DUAStoreRole role, String implementation) {
        return new Builder(id, role, implementation);
    }

    public static final class Builder {
        private final String id;
        private final DUAStoreRole role;
        private final String implementation;
        private final LinkedHashSet<DUAStoreCapability> capabilities = new LinkedHashSet<>();
        private final LinkedHashMap<String, String> parameters = new LinkedHashMap<>();

        private Builder(String id, DUAStoreRole role, String implementation) {
            this.id = Objects.requireNonNull(id, "id");
            this.role = Objects.requireNonNull(role, "role");
            this.implementation = Objects.requireNonNull(implementation, "implementation");
        }

        public Builder capability(DUAStoreCapability capability) {
            capabilities.add(Objects.requireNonNull(capability, "capability"));
            return this;
        }

        public Builder parameter(String name, String value) {
            parameters.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(value, "value"));
            return this;
        }

        public DUABackendStore build() {
            return new DUABackendStore(id, role, implementation, capabilities, parameters);
        }
    }
}
