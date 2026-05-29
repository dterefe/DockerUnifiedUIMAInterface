package org.texttechnologylab.duui.dua;

@FunctionalInterface
public interface DUASource<T> {
    void generate(DUAEmitter<T> emitter) throws Exception;
}
