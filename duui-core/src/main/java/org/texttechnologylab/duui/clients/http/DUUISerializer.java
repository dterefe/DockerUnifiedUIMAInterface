package org.texttechnologylab.duui.clients.http;

import java.io.OutputStream;

@FunctionalInterface
public interface DUUISerializer<T> {
    void serialize(T value, OutputStream output) throws Exception;
}
