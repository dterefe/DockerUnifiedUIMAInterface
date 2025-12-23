package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

public enum DUUIStatus {
    ACTIVE("Active"),
    ANY("Any"),
    CANCELLED("Cancelled"),
    COMPLETED("Completed"),
    DECODE("Decode"),
    DESERIALIZE("Deserialize"),
    SERIALIZE("Serialize"),
    DOWNLOAD("Download"),
    FAILED("Failed"),
    IDLE("Idle"),
    INACTIVE("Inactive"),
    INPUT("Input"),
    INSTANTIATING("Instantiating"),
    OUTPUT("Output"),
    PROCESS("Process"),
    COMPONENT_WAIT("Component Wait"),
    COMPONENT_SERIALIZE("Component Serialize"),
    COMPONENT_DESERIALIZE("Component Deserialize"),
    COMPONENT_PROCESS("Component Process"),
    COMPONENT_LUA_PROCESS("Component Lua Process"),
    SETUP("Setup"),
    SHUTDOWN("Shutdown"),
    SKIPPED("Skipped"),
    IMAGE_START("Starting"),
    UNKNOWN("Unknown"),
    WAITING("Waiting");

    private final String value;

    DUUIStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public String value() {
        return value;
    }

    /**
     * Checks wether the given status is any of the Status names provided as options.
     *
     * @param status  The status to look for.
     * @param options The valid options the status can be.
     * @return If the status is part of options.
     */
    public static boolean oneOf(String status, DUUIStatus... options) {
        if (status == null) {
            return false;
        }
        for (DUUIStatus option : options) {
            if (status.equalsIgnoreCase(option.value)) {
                return true;
            }
        }

        return false;
    }

    public static DUUIStatus fromString(String status) {
        if (status == null) {
            return UNKNOWN;
        }
        for (DUUIStatus option : values()) {
            if (status.equalsIgnoreCase(option.value) || status.equalsIgnoreCase(option.name())) {
                return option;
            }
        }

        return UNKNOWN;
    }

    public static boolean oneOf(String status, String... options) {
        if (status == null) {
            return false;
        }
        for (String option : options) {
            if (status.equalsIgnoreCase(option)) {
                return true;
            }
        }

        return false;
    }
}
