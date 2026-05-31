package org.texttechnologylab.duui.pipeline.io.document;

import org.texttechnologylab.duui.artifact.DUUIArtifact;

import java.io.OutputStream;

@FunctionalInterface
public interface DUUIDocumentSerializer<T> {
    void write(DUUIArtifact<T> artifact, OutputStream output) throws Exception;
}
