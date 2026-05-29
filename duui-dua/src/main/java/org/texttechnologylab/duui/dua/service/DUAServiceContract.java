package org.texttechnologylab.duui.dua.service;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public record DUAServiceContract(
        String id,
        String moduleId,
        String serviceName,
        DUAServiceModuleKind moduleKind,
        DUAServicePerformanceClass performanceClass,
        Set<DUAServiceProtocol> protocols,
        Set<DUAServiceDataClass> ownedData,
        Set<DUAServiceDataClass> derivedData,
        List<DUAServiceInteraction> interactions,
        boolean canonicalAuthority,
        boolean optionalAccelerator,
        String usefulFor,
        String relativePerformance) {
    public DUAServiceContract {
        id = requireText(id, "id");
        moduleId = requireText(moduleId, "moduleId");
        serviceName = requireText(serviceName, "serviceName");
        Objects.requireNonNull(moduleKind, "moduleKind");
        Objects.requireNonNull(performanceClass, "performanceClass");
        protocols = Set.copyOf(Objects.requireNonNull(protocols, "protocols"));
        ownedData = Set.copyOf(Objects.requireNonNull(ownedData, "ownedData"));
        derivedData = Set.copyOf(Objects.requireNonNull(derivedData, "derivedData"));
        interactions = List.copyOf(Objects.requireNonNull(interactions, "interactions"));
        usefulFor = requireText(usefulFor, "usefulFor");
        relativePerformance = requireText(relativePerformance, "relativePerformance");
        if (protocols.isEmpty()) {
            throw new IllegalArgumentException("protocols must not be empty");
        }
    }

    public boolean ownsCanonicalData() {
        return ownedData.stream().anyMatch(data -> switch (data) {
            case CANONICAL_CAS, CANONICAL_CORPUS_REGISTRY, CANONICAL_PAYLOAD -> true;
            default -> false;
        });
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
