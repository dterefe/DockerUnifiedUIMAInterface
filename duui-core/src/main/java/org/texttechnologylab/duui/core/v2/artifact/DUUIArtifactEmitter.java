package org.texttechnologylab.duui.artifact;

@FunctionalInterface
public interface DUUIArtifactEmitter<T> {
    void emit(DUUIArtifact<T> artifact);
}
