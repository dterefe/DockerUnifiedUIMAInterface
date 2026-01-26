package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

public enum DUUIStatus {
    INPUT("Input"),
    DECODE("Decode"),
    SETUP("Setup"),
    SKIPPED("Skipped"),
    INSTANTIATING("Instantiating"),
    IMAGE_START("Starting"),
    WAITING("Waiting"),
    ACTIVE("Active"),
    IDLE("Idle"),
    INACTIVE("Inactive"),
    DESERIALIZE("Deserialize"),
    SERIALIZE("Serialize"),
    PROCESS("Process"),
    COMPONENT_WAIT("Waiting"),
    COMPONENT_SERIALIZE("Serialize"),
    COMPONENT_DESERIALIZE("Deserialize"),
    COMPONENT_PROCESS("Process"),
    COMPONENT_LUA_PROCESS("Process (LUA)"),
    DOWNLOAD("Download"),
    OUTPUT("Output"),
    FAILED("Failed"),
    COMPLETED("Completed"),
    CANCELLED("Cancelled"),
    SHUTDOWN("Shutdown"),
    ANY("Any"),
    UNKNOWN("Unknown");

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
