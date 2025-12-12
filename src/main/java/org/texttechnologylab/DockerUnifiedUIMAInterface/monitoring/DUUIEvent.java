package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIUrlAccessible;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

public class DUUIEvent {

    public static interface Context {

        default String prefix() { return ""; } 

        default Sender sender() {
            return Sender.COMPOSER;
        }

        default String status() {
            return DUUIStatus.UNKNOWN;
        }

        public static PayloadContext typesystem(IDUUIInstantiatedPipelineComponent comp, IDUUIUrlAccessible instance, String typesystem) {
            return new ComponentTypesystemContext(
                Sender.COMPONENT, 
                DUUIStatus.INSTANTIATING, 
                instance.getUniqueInstanceKey(), 
                comp, 
                typesystem
            );
        }

        public static PayloadContext lua(String url, String lua) {
            return new ComponentLuaContext(
                Sender.COMPONENT, 
                DUUIStatus.INSTANTIATING, 
                url, 
                lua
            );
        }

        public static Context from(IDUUIDriverInterface driver, IDUUIInstantiatedPipelineComponent comp) {
            return new ComponentInitializationContext( 
                Sender.DRIVER,
                DUUIStatus.INSTANTIATING,
                comp.getUniqueComponentKey(), 
                driver, 
                comp
            );
        }

        public static Context from(DUUIPipelineDocumentPerformance perf, IDUUIInstantiatedPipelineComponent comp, String instanceId) {
            return new ComponentProcessContext( 
                Sender.COMPONENT,
                DUUIStatus.PROCESS,
                comp.getPipelineComponent().getDriverSimpleName(),
                perf.getRunKey(), 
                perf.getDocument(), 
                comp.getUniqueComponentKey(), 
                instanceId, 
                comp
            );
        }

        public static PayloadContext from(DUUIPipelineDocumentPerformance perf, IDUUIInstantiatedPipelineComponent comp, String instanceId, String logs) {
            return new ComponentProcessErrorContext( 
                Sender.COMPONENT,
                DUUIStatus.PROCESS,
                comp.getPipelineComponent().getDriverSimpleName(),
                perf.getRunKey(), 
                perf.getDocument(), 
                comp.getUniqueComponentKey(), 
                instanceId, 
                comp,
                logs
            );
        }

        public static Context of(Sender sender, String status, Class<?> object) {
            return new DefaultContext(sender, status, object.getSimpleName());
        }

        /**
         * Context for worker threads operating on documents.
         */
        public static Context worker(String runKey, String threadName, String status) {
            return new DocumentWorkerContext(
                Sender.COMPOSER,
                status,
                runKey,
                threadName
            );
        }

        /**
         * Context for worker threads operating on documents.
         */
        public static Context worker(String runKey, String threadName, String status, String payload) {
            return new DocumentWorkerErrorContext(
                Sender.COMPOSER,
                status,
                runKey,
                threadName,
                payload
            );
        }

        /**
         * Context for document pipeline processing (per document and component).
         */
        public static Context pipeline(String runKey, String documentId, String componentName, String driver, String segmentation, String status) {
            return new DocumentPipelineContext(
                Sender.DOCUMENT,
                status,
                runKey,
                documentId,
                componentName,
                driver,
                segmentation
            );
        }

        /**
         * Error context for document pipeline processing.
         */
        public static PayloadContext pipelineError(String runKey, String documentId, String componentName, String driver, String payload) {
            return new DocumentPipelineErrorContext(
                Sender.DOCUMENT,
                DUUIStatus.FAILED,
                runKey,
                documentId,
                componentName,
                driver,
                payload
            );
        }

        /**
         * Document-level lifecycle context (without component details).
         */
        public static Context document(String runKey, String documentId, String status) {
            return new DocumentLifecycleContext(
                Sender.DOCUMENT,
                status,
                runKey,
                documentId
            );
        }

        /**
         * Context for document reader operations (input/output) bound to a specific document.
         */
        public static Context reader(String documentId, DUUICollectionReader source, String status) {
            return new DocumentReaderContext(
                Sender.READER,
                status,
                documentId,
                source.getClass().getSimpleName()
            );
        }

        /**
         * Error context for document reader operations bound to a specific document.
         */
        public static PayloadContext readerError(String documentId, DUUIDocumentReader reader, String payload) {
            return new DocumentReaderErrorContext(
                Sender.READER,
                DUUIStatus.FAILED,
                documentId,
                reader,
                payload
            );
        }

        /**
         * Context for reader-wide lifecycle events (not bound to a single document).
         */
        public static Context readerLifecycle(String source, String readerName, String status) {
            return new DocumentReaderLifecycleContext(
                Sender.READER,
                status,
                source,
                readerName
            );
        }

        /**
         * Context for document storage / handler operations.
         */
        public static Context storage(String backend, String documentId, String path, String status) {
            return new DocumentStorageContext(
                Sender.STORAGE,
                status,
                backend,
                documentId,
                path
            );
        }

        /**
         * Error context for document storage / handler operations.
         */
        public static PayloadContext storageError(String backend, String documentId, String path, String operation, String payload) {
            return new DocumentStorageErrorContext(
                Sender.STORAGE,
                DUUIStatus.FAILED,
                backend,
                documentId,
                path,
                operation,
                payload
            );
        }
    }

    public static interface PayloadContext extends Context {

        String payload();
    } 

    public static class NoContext implements DUUIEvent.Context {

    }

    public static record DefaultContext (
        Sender sender,
        String status, 
        String className
    ) implements Context {}

    public static record ComponentProcessContext (
        Sender sender,
        String status,
        String driver,
        String runKey,
        String documentId,
        String componentId,
        String instanceId,
        IDUUIInstantiatedPipelineComponent component
    ) implements Context {

    }

    public static record ComponentProcessErrorContext (
        Sender sender,
        String status,
        String driver,
        String runKey,
        String documentId,
        String componentId,
        String instanceId,
        IDUUIInstantiatedPipelineComponent component,
        String payload
    ) implements PayloadContext {

    }
    
    public static record ComponentInitializationContext (
        Sender sender,
        String status,
        String componentId,
        IDUUIDriverInterface driver,
        IDUUIInstantiatedPipelineComponent component
    ) implements Context {

    }
    
    public static record ComponentTypesystemContext (
        Sender sender,
        String status,
        String componentId,
        IDUUIInstantiatedPipelineComponent component,
        String typesystem
    ) implements PayloadContext {

        public String payload() {
            return typesystem;
        }
    }
    
    public static record ComponentLuaContext (
        Sender sender,
        String status,
        String url,
        String lua
    ) implements PayloadContext {

        public String payload() {
            return lua;
        }
    }

    /**
     * Worker thread context for document processing.
     */
    public static record DocumentWorkerContext(
        Sender sender,
        String status,
        String runKey,
        String threadName
    ) implements Context {

    }

    /**
     * Worker thread context for document processing.
     */
    public static record DocumentWorkerErrorContext(
        Sender sender,
        String status,
        String runKey,
        String threadName,
        String payload
    ) implements PayloadContext {

    }

    /**
     * Document pipeline processing context (per document and component).
     */
    public static record DocumentPipelineContext(
        Sender sender,
        String status,
        String runKey,
        String documentId,
        String componentName,
        String driver,
        String segmentation
    ) implements Context {

    }

    /**
     * Error context for document pipeline processing.
     */
    public static record DocumentPipelineErrorContext(
        Sender sender,
        String status,
        String runKey,
        String documentId,
        String componentName,
        String driver,
        String payload
    ) implements PayloadContext {

    }

    /**
     * Document-level lifecycle context (without component details).
     */
    public static record DocumentLifecycleContext(
        Sender sender,
        String status,
        String runKey,
        String documentId
    ) implements Context {

    }

    /**
     * Context for document reader operations (input/output) bound to a specific document.
     */
    public static record DocumentReaderContext(
        Sender sender,
        String status,
        String documentId,
        String readerName
    ) implements Context {

    }

    /**
     * Error context for document reader operations bound to a specific document.
     */
    public static record DocumentReaderErrorContext(
        Sender sender,
        String status,
        String documentId,
        DUUIDocumentReader reader,
        String payload
    ) implements PayloadContext {

    }

    /**
     * Reader-wide lifecycle context (input/output not tied to a single document).
     */
    public static record DocumentReaderLifecycleContext(
        Sender sender,
        String status,
        String source,
        String readerName
    ) implements Context {

    }

    /**
     * Context for document storage / handler operations.
     */
    public static record DocumentStorageContext(
        Sender sender,
        String status,
        String backend,
        String documentId,
        String path
    ) implements Context {

    }

    /**
     * Error context for document storage / handler operations.
     */
    public static record DocumentStorageErrorContext(
        Sender sender,
        String status,
        String backend,
        String documentId,
        String path,
        String operation,
        String payload
    ) implements PayloadContext {

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

    private final Sender sender;
    private final String message;
    private final long timestamp;
    private final DUUIComposer.DebugLevel debugLevel;
    private final Context context;

    public DUUIEvent(Sender sender, String message) {
        this(sender, message, Instant.now().toEpochMilli(), DUUIComposer.DebugLevel.NONE, new NoContext());
    }

    public DUUIEvent(Sender sender, String message, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, Instant.now().toEpochMilli(), debugLevel, new NoContext());
    }

    public DUUIEvent(Sender sender, String message, long timestamp) {
        this(sender, message, timestamp, DUUIComposer.DebugLevel.NONE, new NoContext());
    }

    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, timestamp, debugLevel, new NoContext());
    }

    public DUUIEvent(Sender sender, String message, DebugLevel debugLevel, Context context) {
       this(sender, message, Instant.now().toEpochMilli(), debugLevel, context);
    }
    
    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel, DUUIEvent.Context context) {
        this.sender = sender;
        this.message = message;
        this.timestamp = timestamp;
        this.debugLevel = debugLevel;
        this.context = context;
    }



    @Override
    public String toString() {
        String base = String.format("%s %s [%s]: %s",
                timestamp,
                debugLevel.equals(DebugLevel.NONE) ? "" : debugLevel.toString(),
                sender.name(),
                message
        );
        base = StringUtils.stripEnd(base, "\n\r");

        if (context instanceof PayloadContext errorContext) {
            String logs = errorContext.payload();
            if (StringUtils.isNotEmpty(logs)) {
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
}
