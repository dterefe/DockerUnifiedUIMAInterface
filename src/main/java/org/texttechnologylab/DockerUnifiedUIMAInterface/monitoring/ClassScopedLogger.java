package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;

/**
 * DUUI logger that applies per-class filtering and delegates emission
 * to the current thread-local logger in {@link DUUILogContext}.
 */
public final class ClassScopedLogger implements DUUILogger {

    private final String name;

    public ClassScopedLogger(Class<?> clazz) {
        this.name = clazz.getName();
    }

    @Override
    public void emit(DUUIEvent event) {
        if (event == null) {
            return;
        }

        DebugLevel min = DUUILoggingConfig.getMinLevel(name);
        if (event.getDebugLevel().ordinal() < min.ordinal()) {
            return;
        }

        DUUILogger delegate = DUUILogContext.getLogger();
        if (delegate != null && delegate != this) {
            delegate.emit(event);
        }
    }
}

