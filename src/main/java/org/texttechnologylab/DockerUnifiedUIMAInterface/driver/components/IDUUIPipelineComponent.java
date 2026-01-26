package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components;

import java.util.Optional;

import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.model.AnnotatorDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;

public interface IDUUIPipelineComponent extends IDUUIComponent {
    TypeSystemDescription typesystem();

    Optional<String> sourceView();

    Optional<String> targetView();

    Optional<DUUISegmentationStrategy> segmentationStrategy();

    Optional<String> viewName();

    boolean viewInitializeFromInitial();

    Optional<AnnotatorDescriptor> annotatorDescriptor();

    void process(JCas jCas) throws Exception;
}
