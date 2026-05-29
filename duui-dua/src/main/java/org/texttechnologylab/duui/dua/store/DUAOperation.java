package org.texttechnologylab.duui.dua.store;

@FunctionalInterface
public interface DUAOperation<T> {
    T execute() throws Exception;
}
