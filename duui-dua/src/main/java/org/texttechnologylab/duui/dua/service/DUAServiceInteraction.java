package org.texttechnologylab.duui.dua.service;

import java.util.Objects;

public record DUAServiceInteraction(
        String targetServiceId,
        String purpose,
        DUAServiceProtocol protocol,
        boolean requiredForCorrectness) {
    public DUAServiceInteraction {
        targetServiceId = requireText(targetServiceId, "targetServiceId");
        purpose = requireText(purpose, "purpose");
        Objects.requireNonNull(protocol, "protocol");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
