package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.DUUIComponentDescriptors;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.IDUUIComponent;

public interface IDUUIDriver<D extends DUUIComponentDescriptors.IDUUIComponentDescriptor<C>, C extends IDUUIComponent> {
    C instantiate(D descriptor) throws Exception;
}
