package org.texttechnologylab.duui.dua;

@FunctionalInterface
public interface DUAEmitter<T> {
    void emit(DUAArtifact<T> artifact);
}
