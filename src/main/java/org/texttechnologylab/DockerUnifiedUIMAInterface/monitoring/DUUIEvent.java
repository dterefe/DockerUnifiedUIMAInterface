package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIUrlAccessible;
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
    }

    public static interface PayloadContext extends Context {

        String logs();
    } 

    public static class NoContext implements DUUIEvent.Context {

    }

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
        String logs
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

        public String logs() {
            return typesystem;
        }
    }
    
    public static record ComponentLuaContext (
        Sender sender,
        String status,
        String url,
        String lua
    ) implements PayloadContext {

        public String logs() {
            return lua;
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
            String logs = errorContext.logs();
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
