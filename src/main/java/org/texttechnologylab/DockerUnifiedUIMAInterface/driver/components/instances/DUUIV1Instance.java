package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.base.AbstractV1Instance;

public final class DUUIV1Instance extends AbstractV1Instance<DUUIV1Instance> {

    @Override
    public DUUIV1Instance finalization() {
        instanceId();
        endpoint();
        communicationLayer();
        return this;
    }
}
