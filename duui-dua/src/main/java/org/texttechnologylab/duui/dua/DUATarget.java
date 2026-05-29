package org.texttechnologylab.duui.dua;

@FunctionalInterface
public interface DUATarget<T> {
    void accept(DUAArtifact<T> artifact) throws Exception;
}
