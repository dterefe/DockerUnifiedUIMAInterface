package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances;

import java.net.URI;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.IDUUIDescriptor;

public final class DUUIInstanceDescriptors {
    private DUUIInstanceDescriptors() {}

    // ---------------------------
    // Options (driver-facing setters)
    // ---------------------------

    public interface IDUUIInstanceOptions<O extends IDUUIInstanceOptions<O>> {
        O withInstanceId(String instanceId);
    }
    
    public interface IDUUIInstanceDescriptor<I extends IDUUIComponentInstance> extends IDUUIDescriptor<I> {
        String instanceId();
    }

    public interface IDUUIUimaInstanceOptions<O extends IDUUIUimaInstanceOptions<O>> extends IDUUIInstanceOptions<O> {
        O withEngine(AnalysisEngine engine);
    }

    public interface IDUUIUimaInstanceDescriptor<I extends IDUUIComponentInstance> extends IDUUIInstanceDescriptor<I> {
        AnalysisEngine engine();
    }

    public interface IDUUIHttpInstanceDescriptor<I extends IDUUIComponentInstance> extends IDUUIInstanceDescriptor<I> {
        URI endpoint();
    }

    public interface IDUUIHttpInstanceOptions<O extends IDUUIHttpInstanceOptions<O>> extends IDUUIInstanceOptions<O> {
        O withEndpoint(URI endpoint);
    }

    public interface IDUUIV1Instance extends IDUUIComponentInstance {
        URI endpoint();

        IDUUICommunicationLayer communicationLayer();
    }

    public interface IDUUIV1InstanceDescriptor<I extends IDUUIComponentInstance> extends IDUUIHttpInstanceDescriptor<I> {
        IDUUICommunicationLayer communicationLayer();
    }

    public interface IDUUIV1InstanceOptions<O extends IDUUIV1InstanceOptions<O>> extends IDUUIHttpInstanceOptions<O> {
        O withCommunicationLayer(IDUUICommunicationLayer communicationLayer);
    }

    public interface IDUUIContainerInstanceDescriptor<I extends IDUUIComponentInstance> extends IDUUIHttpInstanceDescriptor<I> {
        String containerId();
    }

    public interface IDUUIContainerInstanceOptions<O extends IDUUIContainerInstanceOptions<O>> extends IDUUIHttpInstanceOptions<O> {
        O withContainerId(String containerId);
    }
    
}
