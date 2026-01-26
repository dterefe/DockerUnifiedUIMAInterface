package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components;

import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.DUUIComponentDescriptors.IDUUIUimaComponentDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.base.AbstractPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIUimaInstance;

public final class DUUIUimaComponent extends AbstractPipelineComponent<DUUIUimaInstance, DUUIUimaComponent>
        implements IDUUIUimaComponentDescriptor<IDUUIPipelineComponent> {

    private TypeSystemDescription typesystem;

    @Override
    public DUUIInstanceDescriptors.IDUUIUimaInstanceOptions<DUUIUimaInstance> createUimaInstance() {
        DUUIUimaInstance instance = new DUUIUimaInstance();
        pending.add(instance);
        return instance;
    }

    @Override
    public DUUIUimaComponent finalization() throws Exception {
        for (var descriptor : pending) {
            DUUIUimaInstance finalized = descriptor.finalization();
            pool.add(finalized);
        }
        pending.clear();
        markFinalized();
        return this;
    }

    @Override
    public TypeSystemDescription typesystem() {
        return typesystem;
    }

    @Override
    protected void processWithInstance(JCas jCas, DUUIUimaInstance instance) throws Exception {
        instance.engine().process(jCas);
    }
}
