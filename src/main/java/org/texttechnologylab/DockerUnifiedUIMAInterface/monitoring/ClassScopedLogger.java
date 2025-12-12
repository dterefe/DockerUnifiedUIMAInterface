package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;

/**
 * DUUI logger that applies per-class filtering and delegates emission
 * to the current thread-local logger in {@link DUUILogContext}.
 * <p>
 * The logger derives its logical {@link #name()} and default
 * {@link #sender()} from the associated class so that higher-level
 * components can reason about the origin of log events.
 */
public final class ClassScopedLogger implements DUUILogger {

    private final String name;
    private final DUUIEvent.Sender sender;
    private volatile DUUIEventEmitter delegate;

    public ClassScopedLogger(Class<?> clazz) {
        this.name = clazz.getName();
        this.sender = resolveSender(clazz);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DUUIEvent.Sender sender() {
        return sender;
    }

    /**
     * Sets the delegate emitter for this class-scoped logger. When non-null,
     * all events passing the class-level filter are forwarded to this emitter.
     */
    public void setDelegate(DUUIEventEmitter delegate) {
        this.delegate = delegate;
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

        DUUIEventEmitter target = this.delegate;
        if (target != null && target != this) {
            target.emit(event);
            return;
        }

        DUUILogger ctxLogger = DUUILogContext.getLogger();
        if (ctxLogger != null && ctxLogger != this) {
            ctxLogger.emit(event);
        }
    }

    private static DUUIEvent.Sender resolveSender(Class<?> clazz) {
        if (clazz == null) {
            return DUUIEvent.Sender.SYSTEM;
        }

        if (DUUIComposer.class.isAssignableFrom(clazz)) {
            return DUUIEvent.Sender.COMPOSER;
        }

        if (IDUUIDriverInterface.class.isAssignableFrom(clazz)) {
            return DUUIEvent.Sender.DRIVER;
        }

        if (IDUUIInstantiatedPipelineComponent.class.isAssignableFrom(clazz)) {
            return DUUIEvent.Sender.COMPONENT;
        }

        if (DUUICollectionReader.class.isAssignableFrom(clazz)
                || IDUUIStorageBackend.class.isAssignableFrom(clazz)
                || clazz.getName().contains(".io.reader.")) {
            return DUUIEvent.Sender.READER;
        }

        if (IDUUIDocumentHandler.class.isAssignableFrom(clazz)
                || clazz.getName().contains(".document_handler.")) {
            return DUUIEvent.Sender.STORAGE;
        }

        if (DUUIDocument.class.isAssignableFrom(clazz)) {
            return DUUIEvent.Sender.DOCUMENT;
        }

        return DUUIEvent.Sender.SYSTEM;
    }
}
