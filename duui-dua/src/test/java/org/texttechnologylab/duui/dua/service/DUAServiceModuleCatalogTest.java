package org.texttechnologylab.duui.dua.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

class DUAServiceModuleCatalogTest {
    private final DUAServiceModuleCatalog catalog = DUAServiceModuleCatalog.coreCatalog();

    @Test
    void coreServicesAreCanonicalAuthorities() {
        Set<String> authorities = catalog.canonicalAuthorities().stream()
                .map(DUAServiceContract::id)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        assertEquals(Set.of(
                DUAServiceModuleCatalog.CORE_REGISTRY_SERVICE_ID,
                DUAServiceModuleCatalog.CORE_CAS_SERVICE_ID,
                DUAServiceModuleCatalog.CORE_PIPELINE_WINDOW_SERVICE_ID), authorities);
    }

    @Test
    void acceleratorsNeverOwnCanonicalCasData() {
        for (DUAServiceContract service : catalog.servicesByKind(DUAServiceModuleKind.QUERY_ACCELERATOR)) {
            assertTrue(service.optionalAccelerator(), service.id());
            assertFalse(service.canonicalAuthority(), service.id());
            assertFalse(service.ownsCanonicalData(), service.id());
            assertTrue(service.derivedData().stream().anyMatch(data -> data.name().startsWith("DERIVED_")),
                    service.id());
        }
    }

    @Test
    void everyOptionalAcceleratorCanRefreshFromEventsAndBackfillFromCore() {
        for (DUAServiceContract service : catalog.services()) {
            if (!service.optionalAccelerator() || service.moduleKind() == DUAServiceModuleKind.INSPECTOR
                    || service.moduleKind() == DUAServiceModuleKind.OBSERVABILITY) {
                continue;
            }

            assertTrue(dependsOn(service, DUAServiceModuleCatalog.EVENT_LOG_SERVICE_ID), service.id());
            assertTrue(dependsOn(service, DUAServiceModuleCatalog.CORE_REGISTRY_SERVICE_ID), service.id());
            assertTrue(dependsOn(service, DUAServiceModuleCatalog.CORE_CAS_SERVICE_ID), service.id());
        }
    }

    @Test
    void serviceCatalogUsesPluggableProtocolsInsteadOfOneGlobalTransport() {
        Set<DUAServiceProtocol> protocols = catalog.services().stream()
                .flatMap(service -> service.protocols().stream())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        assertTrue(protocols.contains(DUAServiceProtocol.IN_PROCESS));
        assertTrue(protocols.contains(DUAServiceProtocol.GRPC));
        assertTrue(protocols.contains(DUAServiceProtocol.HTTP_OPENAPI));
        assertTrue(protocols.contains(DUAServiceProtocol.EVENT_STREAM));
        assertTrue(protocols.contains(DUAServiceProtocol.ARROW_FLIGHT));
        assertTrue(protocols.size() >= 6);
    }

    @Test
    void allServiceInteractionsPointToKnownServices() {
        Set<String> serviceIds = catalog.services().stream()
                .map(DUAServiceContract::id)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        for (DUAServiceContract service : catalog.services()) {
            for (DUAServiceInteraction interaction : service.interactions()) {
                assertTrue(serviceIds.contains(interaction.targetServiceId()),
                        service.id() + " -> " + interaction.targetServiceId());
                assertNotEquals(service.id(), interaction.targetServiceId(), service.id());
            }
        }
    }

    @Test
    void queryCoordinatorFederatesMultipleWeakPointModules() {
        DUAServiceContract coordinator = catalog.findService("dua-query.coordinator").orElseThrow();

        assertTrue(dependsOn(coordinator, "dua-fulltext.text-index"));
        assertTrue(dependsOn(coordinator, "dua-annotation-analytics.annotation-facts"));
        assertTrue(dependsOn(coordinator, "dua-metadata-ontology.metadata-index"));
        assertTrue(dependsOn(coordinator, "dua-semantic-events.event-index"));
        assertTrue(dependsOn(coordinator, "dua-geo-temporal.spacetime-index"));
        assertTrue(dependsOn(coordinator, "dua-vector.vector-index"));
    }

    private static boolean dependsOn(DUAServiceContract service, String targetServiceId) {
        return service.interactions().stream()
                .anyMatch(interaction -> interaction.targetServiceId().equals(targetServiceId));
    }
}
