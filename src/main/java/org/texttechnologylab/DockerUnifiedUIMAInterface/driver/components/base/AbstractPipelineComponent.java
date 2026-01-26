package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.IDUUIDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.IDUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.IDUUIComponentInstance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.model.AnnotatorDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;

public abstract class AbstractPipelineComponent<I extends IDUUIComponentInstance, SELF extends AbstractPipelineComponent<I, SELF>>
        extends AbstractComponentDescriptor<IDUUIPipelineComponent, SELF>
        implements IDUUIPipelineComponent {

    protected final BlockingQueue<I> pool = new LinkedBlockingQueue<>();
    protected final List<IDUUIDescriptor<? extends I>> pending = new ArrayList<>();
    protected Optional<AnnotatorDescriptor> annotatorDescriptor = Optional.empty();

    @Override
    public final Optional<AnnotatorDescriptor> annotatorDescriptor() {
        return annotatorDescriptor;
    }

    @Override
    public final void process(JCas jCas) throws Exception {
        I inst = pool.take();
        try {
            status = DUUIStatus.ACTIVE;
            processWithInstance(jCas, inst);
        } finally {
            pool.add(inst);
        }
    }

    protected abstract void processWithInstance(JCas jCas, I instance) throws Exception;
}
