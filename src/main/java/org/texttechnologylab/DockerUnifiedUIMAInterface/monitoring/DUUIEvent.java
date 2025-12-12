package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Instant;
import java.util.Optional;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.exception.ExceptionUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIUrlAccessible;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

public class DUUIEvent {

    public interface Context {

        public static Context typesystem(IDUUIInstantiatedPipelineComponent comp, IDUUIUrlAccessible instance, String typesystem) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.INSTANTIATING)
                .withComponentId(comp.getUniqueComponentKey())
                .withComponentName(comp.getName())
                .withComponentId(instance.getUniqueInstanceKey())
                .withTypesystem(typesystem)
                .build();
        }

        public static Context lua(String url, String lua) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.INSTANTIATING)
                .withUrl(url)
                .withLua(lua)
                .build();
        }

        public static Context driver(IDUUIDriverInterface driver, IDUUIInstantiatedPipelineComponent comp) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.INSTANTIATING)
                .withComponentId(comp.getUniqueComponentKey())
                .withDriver(driver != null ? driver.getClass().getSimpleName() : null)
                .build();
        }

        public static Context component(DUUIPipelineDocumentPerformance perf, IDUUIInstantiatedPipelineComponent comp, String instanceId) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.PROCESS)
                .withDriver(comp.getPipelineComponent().getDriverSimpleName())
                .withRunKey(perf.getRunKey())
                .withDocument(perf.getDocument())
                .withComponentId(comp.getUniqueComponentKey())
                .withInstanceId(instanceId)
                .build();
        }

        public static Context component(DUUIPipelineDocumentPerformance perf, IDUUIInstantiatedPipelineComponent comp, String instanceId, String logs) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.PROCESS)
                .withDriver(comp.getPipelineComponent().getDriverSimpleName())
                .withRunKey(perf.getRunKey())
                .withDocument(perf.getDocument())
                .withComponentId(comp.getUniqueComponentKey())
                .withInstanceId(instanceId)
                .withPayload(logs)
                .build();
        }

        public static Context of(String status) {
            return new ContextBuilder()
                .withStatus(status)
                .build();
        }

        /**
         * Context for worker threads operating on documents.
         */
        public static Context worker(String runKey, String status) {
            return new ContextBuilder()
                .withStatus(status)
                .withRunKey(runKey)
                .build();
        }

        /**
         * Context for worker threads operating on documents.
         */
        public static Context worker(String runKey, String status, String payload) {
            return new ContextBuilder()
                .withStatus(status)
                .withRunKey(runKey)
                .withPayload(payload)
                .build();
        }

        /**
         * Context for document pipeline processing (per document and component).
         */
        public static Context pipeline(String runKey, String documentId, String componentName, String driver, String segmentation, String status) {
            return new ContextBuilder()
                .withThreadName(Thread.currentThread().getName())
                .withStatus(status)
                .withRunKey(runKey)
                .withDocument(documentId)
                .withComponentName(componentName)
                .withDriver(driver)
                .withSegmentation(segmentation)
                .build();
        }

        /**
         * Error context for document pipeline processing.
         */
        public static Context pipelineError(String runKey, String documentId, String componentName, String driver, String payload) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.FAILED)
                .withRunKey(runKey)
                .withDocument(documentId)
                .withComponentName(componentName)
                .withDriver(driver)
                .withPayload(payload)
                .build();
        }

        /**
         * Document-level general context
         */
        public static Context document(String documentId, String status) {
            return new ContextBuilder()
                .withStatus(status)
                .withDocument(documentId)
                .build();
        }

        /**
         * Document-level lifecycle context (without component details).
         */
        public static Context document(String runKey, String documentId, String status) {
            return new ContextBuilder()
                .withStatus(status)
                .withRunKey(runKey)
                .withDocument(documentId)
                .build();
        }

        /**
         * Context for document reader operations (input/output) bound to a specific document.
         */
        public static Context reader(String documentId, String status) {
            return new ContextBuilder()
                .withStatus(status)
                .withDocument(documentId)
                .build();
        }

        /**
         * Error context for document reader operations bound to a specific document.
         */
        public static Context readerError(String documentId, String payload) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.FAILED)
                .withDocument(documentId)
                .withPayload(payload)
                .build();
        }
        
        public static Context readerError(String documentId, Exception e) {
            return readerError(documentId, ExceptionUtils.getStackTrace(e));
        }

        /**
         * Context for document storage / handler operations.
         */
        public static Context storage(String backend, String documentId, String path, String status) {
            return new ContextBuilder()
                .withStatus(status)
                .withBackend(backend)
                .withDocument(documentId)
                .withPath(path)
                .build();
        }

        /**
         * Error context for document storage / handler operations.
         */
        public static Context storage(String backend, String documentId, String path, String operation, String payload) {
            return new ContextBuilder()
                .withStatus(DUUIStatus.FAILED)
                .withBackend(backend)
                .withDocument(documentId)
                .withPath(path)
                .withOperation(operation)
                .withPayload(payload)
                .build();
        }
    }
    
    public enum Sender {
        SYSTEM,
        COMPOSER,
        DRIVER,
        COMPONENT,
        HANDLER,
        DOCUMENT,
        STORAGE,
        READER,
        WRITER,
        MOINTOR;
    }

    private final String className;
    private final Sender sender;
    private final String message;
    private final long timestamp;
    private final DUUIComposer.DebugLevel debugLevel;
    private final Context context;

    public DUUIEvent(Sender sender, String message) {
        this(sender, message, Instant.now().toEpochMilli(), DUUIComposer.DebugLevel.NONE, new NoContext(), "");
    }

    public DUUIEvent(Sender sender, String message, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, Instant.now().toEpochMilli(), debugLevel, new NoContext(), "");
    }

    public DUUIEvent(Sender sender, String message, long timestamp) {
        this(sender, message, timestamp, DUUIComposer.DebugLevel.NONE, new NoContext(), "");
    }

    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, timestamp, debugLevel, new NoContext(), "");
    }

    public DUUIEvent(Sender sender, String message, DebugLevel debugLevel, Context context, String name) {
       this(sender, message, Instant.now().toEpochMilli(), debugLevel, context, name);
    }
    
    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel, DUUIEvent.Context context, String name) {
        this.sender = sender;
        this.message = message;
        this.timestamp = timestamp;
        this.debugLevel = debugLevel;
        this.context = context;
        this.className = name;
    }

    @Override
    public String toString() {
        String ts = String.valueOf(timestamp);
        if (DUUILoggingConfig.isFormatTimestamp()) {
            ts = Instant.ofEpochMilli(timestamp).toString();
        }

        String levelPart = debugLevel.equals(DebugLevel.NONE) ? "" : debugLevel.toString();
        String senderPart = DUUILoggingConfig.isIncludeSender() ? sender.name() : "";
        String classPart = DUUILoggingConfig.isIncludeClassName() && className != null && !className.isEmpty()
                ? className
                : "";

        String base = String.format("%s %s %s %s %s",
                ts,
                levelPart,
                senderPart,
                classPart,
                message
        );
        base = StringUtils.stripEnd(base, "\n\r");

        if (DUUILoggingConfig.isIncludePayload() && context instanceof DefaultContext ctx) {
            var logsOpt = ctx.payload().filter(StringUtils::isNotEmpty);
            if (logsOpt.isPresent()) {
                String logs = logsOpt.get();
                String indented = "    " + logs.replace("\n", "\n    ");
                indented = StringUtils.stripEnd(indented, "\n\r");
                return base + "\n" + indented + "\n";
            }
        }

        return base + "\n";
    }

    public Sender getSender() {
        return sender;
    }

    public String getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DUUIComposer.DebugLevel getDebugLevel() {
        return debugLevel;
    }

    public Context getContext() {
        return context;
    }

    public static final class NoContext implements DUUIEvent.Context { 
    }

    public static record DefaultContext(
        Optional<String> status,
        Optional<String> driver,
        Optional<String> runKey,
        Optional<String> document,
        Optional<String> componentId,
        Optional<String> instanceId,
        Optional<String> componentName,
        Optional<String> threadName,
        Optional<String> readerName,
        Optional<String> backend,
        Optional<String> path,
        Optional<String> segmentation,
        Optional<String> operation,
        Optional<String> url,
        Optional<String> typesystem,
        Optional<String> lua,
        Optional<String> payload
    ) implements Context {

        public DefaultContext {
            status       = Objects.requireNonNullElse(status,       Optional.empty());
            driver       = Objects.requireNonNullElse(driver,       Optional.empty());
            runKey       = Objects.requireNonNullElse(runKey,       Optional.empty());
            document     = Objects.requireNonNullElse(document,     Optional.empty());
            componentId  = Objects.requireNonNullElse(componentId,  Optional.empty());
            instanceId   = Objects.requireNonNullElse(instanceId,   Optional.empty());
            componentName= Objects.requireNonNullElse(componentName,Optional.empty());
            threadName   = Objects.requireNonNullElse(threadName,   Optional.empty());
            readerName   = Objects.requireNonNullElse(readerName,   Optional.empty());
            backend      = Objects.requireNonNullElse(backend,      Optional.empty());
            path         = Objects.requireNonNullElse(path,         Optional.empty());
            segmentation = Objects.requireNonNullElse(segmentation, Optional.empty());
            operation    = Objects.requireNonNullElse(operation,    Optional.empty());
            url          = Objects.requireNonNullElse(url,          Optional.empty());
            typesystem   = Objects.requireNonNullElse(typesystem,   Optional.empty());
            lua          = Objects.requireNonNullElse(lua,          Optional.empty());
            payload      = Objects.requireNonNullElse(payload,      Optional.empty());
        }

        public String statusOrUnknown() {
            return status
                .filter(StringUtils::isNotEmpty)
                .orElse(DUUIStatus.UNKNOWN);
        }
    }

    
    public static final class ContextBuilder {

        private Optional<String> status = Optional.empty();
        private Optional<String> driver = Optional.empty();
        private Optional<String> runKey = Optional.empty();
        private Optional<String> document = Optional.empty();
        private Optional<String> componentId = Optional.empty();
        private Optional<String> instanceId = Optional.empty();
        private Optional<String> componentName = Optional.empty();
        private Optional<String> threadName = Optional.empty();
        private Optional<String> readerName = Optional.empty();
        private Optional<String> backend = Optional.empty();
        private Optional<String> path = Optional.empty();
        private Optional<String> segmentation = Optional.empty();
        private Optional<String> operation = Optional.empty();
        private Optional<String> url = Optional.empty();
        private Optional<String> typesystem = Optional.empty();
        private Optional<String> lua = Optional.empty();
        private Optional<String> payload = Optional.empty();

        private static Optional<String> opt(String value) {
            return Optional.ofNullable(value).filter(StringUtils::isNotEmpty);
        }

        public ContextBuilder withStatus(String status) {
            this.status = opt(status);
            return this;
        }

        public ContextBuilder withDriver(String driver) {
            this.driver = opt(driver);
            return this;
        }

        public ContextBuilder withRunKey(String runKey) {
            this.runKey = opt(runKey);
            return this;
        }

        public ContextBuilder withDocument(String document) {
            this.document = opt(document);
            return this;
        }

        public ContextBuilder withComponentId(String componentId) {
            this.componentId = opt(componentId);
            return this;
        }

        public ContextBuilder withInstanceId(String instanceId) {
            this.instanceId = opt(instanceId);
            return this;
        }

        public ContextBuilder withComponentName(String componentName) {
            this.componentName = opt(componentName);
            return this;
        }

        public ContextBuilder withThreadName(String threadName) {
            this.threadName = opt(threadName);
            return this;
        }

        public ContextBuilder withReaderName(String readerName) {
            this.readerName = opt(readerName);
            return this;
        }

        public ContextBuilder withBackend(String backend) {
            this.backend = opt(backend);
            return this;
        }

        public ContextBuilder withPath(String path) {
            this.path = opt(path);
            return this;
        }

        public ContextBuilder withSegmentation(String segmentation) {
            this.segmentation = opt(segmentation);
            return this;
        }

        public ContextBuilder withOperation(String operation) {
            this.operation = opt(operation);
            return this;
        }

        public ContextBuilder withUrl(String url) {
            this.url = opt(url);
            return this;
        }

        public ContextBuilder withTypesystem(String typesystem) {
            this.typesystem = opt(typesystem);
            return this;
        }

        public ContextBuilder withLua(String lua) {
            this.lua = opt(lua);
            return this;
        }

        public ContextBuilder withPayload(String payload) {
            this.payload = opt(payload);
            return this;
        }

        public DefaultContext build() {
            withThreadName(Thread.currentThread().getName());

            return new DefaultContext(
                status,
                driver,
                runKey,
                document,
                componentId,
                instanceId,
                componentName,
                threadName,
                readerName,
                backend,
                path,
                segmentation,
                operation,
                url,
                typesystem,
                lua,
                payload
            );
        }
    }
}
