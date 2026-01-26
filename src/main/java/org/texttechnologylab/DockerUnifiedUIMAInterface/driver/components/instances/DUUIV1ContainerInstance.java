package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors.IDUUIContainerInstanceDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors.IDUUIContainerInstanceOptions;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.base.AbstractV1Instance;

public final class DUUIV1ContainerInstance
        extends AbstractV1Instance<DUUIV1ContainerInstance>
        implements IDUUIContainerInstanceOptions<DUUIV1ContainerInstance>,
        IDUUIContainerInstanceDescriptor<DUUIV1ContainerInstance> {

    @Override
    public DUUIV1ContainerInstance finalization() {
        instanceId();
        endpoint();
        communicationLayer();
        containerId();
        return this;
    }
}
