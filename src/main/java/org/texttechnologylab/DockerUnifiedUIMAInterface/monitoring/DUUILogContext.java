package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.util.Objects;

/**
 * Global logging context for DUUI.
 * <p>
 * Holds the current {@link DUUILogger} and {@link DUUIEvent.Context}
 * in thread-local storage so that logging can work from anywhere in the call
 * stack (including static helper methods) without having to thread a logger
 * instance through every method signature.
 */
public final class DUUILogContext {

    private static final DUUIEvent.Context DEFAULT_CONTEXT = new DUUIEvent.NoContext();

    private static final ThreadLocal<DUUIEvent.Context> CURRENT_CONTEXT =
            ThreadLocal.withInitial(() -> DEFAULT_CONTEXT);

    /**
     * Default logger used when no logger has been set in this thread.
     * Emits events directly to {@code System.out}/{@code System.err}.
     */
    private static final DUUILogger FALLBACK_LOGGER = (DUUIEvent event) -> {
        if (event == null) {
            return;
        }
        switch (event.getDebugLevel()) {
            case CRITICAL, ERROR, DEBUG -> System.err.println(event);
            default -> System.out.println(event);
        }
    };

    private static final ThreadLocal<DUUILogger> CURRENT_LOGGER =
            ThreadLocal.withInitial(() -> FALLBACK_LOGGER);

    private DUUILogContext() {
        // utility
    }

    public static DUUIEvent.Context getContext() {
        DUUIEvent.Context context = CURRENT_CONTEXT.get();
        return context != null ? context : DEFAULT_CONTEXT;
    }

    public static void setContext(DUUIEvent.Context context) {
        CURRENT_CONTEXT.set(Objects.requireNonNullElse(context, DEFAULT_CONTEXT));
    }

    public static void clear() {
        CURRENT_CONTEXT.remove();
        CURRENT_LOGGER.remove();
    }

    /**
     * Returns the current logger for this thread. This logger uses the
     * thread-local emitter and context managed by this class.
     */
    public static DUUILogger getLogger() {
        DUUILogger logger = CURRENT_LOGGER.get();
        return logger != null ? logger : FALLBACK_LOGGER;
    }

    public static void setLogger(DUUILogger logger) {
        CURRENT_LOGGER.set(logger != null ? logger : FALLBACK_LOGGER);
    }
}
