package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components;

import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;

public interface IDUUIComponent extends DUUIComponentDescriptors.IDUUIComponentMeta {
    DUUIStatus status();
    String finalizedRepresentation();
    int finalizedRepresentationHash();
}
