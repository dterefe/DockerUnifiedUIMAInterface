package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors.IDUUIUimaInstanceDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors.IDUUIUimaInstanceOptions;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.base.AbstractInstance;

public final class DUUIUimaInstance extends AbstractInstance<DUUIUimaInstance>
        implements IDUUIComponentInstance,
        IDUUIUimaInstanceOptions<DUUIUimaInstance>,
        IDUUIUimaInstanceDescriptor<DUUIUimaInstance> {

    @Override
    public DUUIUimaInstance finalization() {
        instanceId();
        engine();
        return this;
    }
}
