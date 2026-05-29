package org.texttechnologylab.duui.dua.service;

import java.util.List;
import java.util.Objects;

public record DUAServiceModule(
        String id,
        String name,
        DUAServiceModuleKind kind,
        String responsibility,
        List<DUAServiceContract> services) {
    public DUAServiceModule {
        id = requireText(id, "id");
        name = requireText(name, "name");
        Objects.requireNonNull(kind, "kind");
        responsibility = requireText(responsibility, "responsibility");
        services = List.copyOf(Objects.requireNonNull(services, "services"));
        if (services.isEmpty()) {
            throw new IllegalArgumentException("services must not be empty");
        }
        for (DUAServiceContract service : services) {
            if (!id.equals(service.moduleId())) {
                throw new IllegalArgumentException("service " + service.id() + " is assigned to " + service.moduleId()
                        + " but listed in module " + id);
            }
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
