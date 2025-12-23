package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.util.Objects;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;

/**
 * Global logging context for DUUI.
 * <p>
 * Holds the current {@link DUUILogger} and {@link DUUIContext}
 * in thread-local storage so that logging can work from anywhere in the call
 * stack (including static helper methods) without having to thread a logger
 * instance through every method signature.
 */
public final class DUUILogContext {

    private static final DUUIContext DEFAULT_CONTEXT = DUUIContexts.defaultContext();

    private static final ThreadLocal<DUUIContext> CURRENT_CONTEXT =
            ThreadLocal.withInitial(() -> DEFAULT_CONTEXT);

    /**
     * Default logger used when no logger has been set in this thread.
     * Emits events directly to {@code System.out}/{@code System.err}, but
     * still respects the global logging configuration in
     * {@link DUUILoggingConfig}.
     */
    private static final DUUILogger FALLBACK_LOGGER = (DUUIEvent event) -> {
        if (event == null) {
            return;
        }
        DebugLevel min = DUUILoggingConfig.getMinLevel("fallback");
        if (event.getDebugLevel().ordinal() < min.ordinal()) {
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

    public static DUUIContext getContext() {
        DUUIContext context = CURRENT_CONTEXT.get();
        return context != null ? context : DEFAULT_CONTEXT;
    }

    public static void setContext(DUUIContext context) {
        CURRENT_CONTEXT.set(Objects.requireNonNullElse(context, DEFAULT_CONTEXT));
    }

    public static void clear() {
        CURRENT_CONTEXT.remove();
        CURRENT_LOGGER.remove();
        DUUIContextTL.clear();
    }

    /**
     * Returns the current logger for this thread. This logger uses the
     * thread-local emitter and context managed by this class.
     */
    public static DUUILogger getLogger() {
        DUUILogger logger = CURRENT_LOGGER.get();
        return logger != null ? logger : FALLBACK_LOGGER;
    }

    public static void setLogger(DUUIComposer composer) {
        CURRENT_LOGGER.set(composer.getLogger() != null ? composer.getLogger() : FALLBACK_LOGGER);
        DUUIContexts.setComposerContext(composer.getLogger().getContext());
        setContext(composer.getLogger().getContext());
    }

    public static void setLogger(DUUILogger logger) {
        CURRENT_LOGGER.set(logger != null ? logger : FALLBACK_LOGGER);
    }
}
