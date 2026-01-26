package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.base;

import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors;

public abstract class AbstractV1Instance<SELF extends AbstractV1Instance<SELF>>
        extends AbstractInstance<SELF>
        implements DUUIInstanceDescriptors.IDUUIV1Instance,
        DUUIInstanceDescriptors.IDUUIV1InstanceOptions<SELF>,
        DUUIInstanceDescriptors.IDUUIV1InstanceDescriptor<SELF> {

    private IDUUICommunicationLayer communicationLayer;

    @Override
    public final SELF withCommunicationLayer(IDUUICommunicationLayer communicationLayer) {
        this.communicationLayer = communicationLayer;
        return self();
    }

    @Override
    public final IDUUICommunicationLayer communicationLayer() {
        if (communicationLayer == null) {
            throw new IllegalStateException("communicationLayer missing");
        }
        return communicationLayer;
    }
}
