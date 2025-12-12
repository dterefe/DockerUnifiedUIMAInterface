package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

/**
 * Factory for class-scoped DUUI loggers.
 */
public final class DUUILoggers {

    private DUUILoggers() {
    }

    public static DUUILogger getLogger(Class<?> clazz) {
        return new ClassScopedLogger(clazz);
    }
}

