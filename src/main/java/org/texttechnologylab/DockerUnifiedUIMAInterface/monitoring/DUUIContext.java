package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;

public sealed interface DUUIContext extends PayloadUtil
    permits DUUIContext.DefaultContext,
            DUUIContext.ComposerContext,
            DUUIContext.WorkerContext,
            DUUIContext.DocumentContext,
            DUUIContext.DocumentProcessContext,
            DUUIContext.ComponentContext,
            DUUIContext.InstantiatedComponentContext,
            DUUIContext.DocumentComponentProcessContext,
            DUUIContext.ReaderContext,
            DUUIContext.ReaderDocumentContext,
            DUUIContext.DriverContext {

    @Nonnull
    default DUUIContext updateStatus(@Nonnull DUUIStatus status) {
        return DUUIContexts.updateStatus(this, status);
    }

    @Nonnull
    default DUUIContext updatePayload(DUUIContext.Payload payload) {
        return DUUIContexts.updateStatus(this, payload);
    }

    public enum PayloadKind {
        STACKTRACE,
        LUA,
        TYPESYSTEM,
        RESPONSE,
        LOGS,
        GENERIC,
        METRIC_MILLIS,
        NONE
    }
    
    public static record Payload(
        @Nonnull
        DUUIStatus status,
        @Nonnull
        String content,
        @Nonnull
        PayloadKind kind,
        @Nonnull
        String thread
    ) {}

    public static record DefaultContext(
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public DefaultContext {
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record ComposerContext(
        @Nonnull String runKey,
        @Nonnull Map<String, DUUIStatus> pipelineStatus,
        @Nonnull AtomicInteger progressAtomic,
        int total,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public ComposerContext {
            runKey = Objects.requireNonNull(runKey, "runKey");
            pipelineStatus = Map.copyOf(Objects.requireNonNull(pipelineStatus, "pipelineStatus"));
            progressAtomic = Objects.requireNonNull(progressAtomic, "progressAtomic");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record WorkerContext(
        @Nonnull ComposerContext composer,
        @Nonnull String name,
        @Nonnull AtomicInteger activeWorkers,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public WorkerContext {
            composer = Objects.requireNonNull(composer, "composer");
            name = Objects.requireNonNull(name, "name");
            activeWorkers = Objects.requireNonNull(activeWorkers, "activeWorkers");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record DocumentContext(
        @Nonnull DUUIDocument document,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public DocumentContext {
            document = Objects.requireNonNull(document, "document");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record DocumentProcessContext(
        @Nonnull DocumentContext document,
        @Nonnull ComposerContext composer,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public DocumentProcessContext {
            document = Objects.requireNonNull(document, "document");
            composer = Objects.requireNonNull(composer, "composer");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record ComponentContext(
        @Nonnull String componentId,
        @Nonnull String componentName,
        @Nonnull String driverName,
        @Nonnull List<String> instanceIds,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public ComponentContext {
            componentId = Objects.requireNonNull(componentId, "componentId");
            componentName = Objects.requireNonNull(componentName, "componentName");
            driverName = Objects.requireNonNull(driverName, "driverName");
            instanceIds = List.copyOf(Objects.requireNonNull(instanceIds, "instanceIds"));
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record InstantiatedComponentContext(
        @Nonnull ComponentContext component,
        @Nonnull String instanceId,
        @Nonnull String endpoint,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public InstantiatedComponentContext {
            component = Objects.requireNonNull(component, "component");
            instanceId = Objects.requireNonNull(instanceId, "instanceId");
            endpoint = Objects.requireNonNull(endpoint, "endpoint");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record DocumentComponentProcessContext(
        @Nonnull DocumentProcessContext document,
        @Nonnull InstantiatedComponentContext component,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public DocumentComponentProcessContext {
            document = Objects.requireNonNull(document, "document");
            component = Objects.requireNonNull(component, "component");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record ReaderContext(
        @Nonnull DUUIDocumentReader reader,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public ReaderContext {
            reader = Objects.requireNonNull(reader, "reader");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record ReaderDocumentContext(
        @Nonnull ReaderContext reader,
        @Nonnull DocumentContext document,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public ReaderDocumentContext {
            reader = Objects.requireNonNull(reader, "reader");
            document = Objects.requireNonNull(document, "document");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

    public static record DriverContext(
        @Nonnull String driverName,
        @Nonnull
        Payload payloadRecord
    ) implements DUUIContext {
        public DriverContext {
            driverName = Objects.requireNonNull(driverName, "driverName");
            payloadRecord = Objects.requireNonNull(payloadRecord, "payloadRecord");
        }
    }

}

interface PayloadUtil {
    @Nonnull
    default DUUIStatus status() {
        return payloadRecord().status();
    }

    @Nonnull
    default String thread() {
        return payloadRecord().thread();
    }

    @Nonnull
    DUUIContext.Payload payloadRecord();

    default boolean hasPayload() {
        return payloadKind() != DUUIContext.PayloadKind.NONE;
    }

    @Nonnull
    default String payload() {
        return payloadRecord().content();
    }

    @Nonnull
    default DUUIContext.PayloadKind payloadKind() {
        return payloadRecord().kind();
    }
}
