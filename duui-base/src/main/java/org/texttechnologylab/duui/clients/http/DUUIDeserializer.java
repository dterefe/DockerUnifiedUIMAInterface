package org.texttechnologylab.duui.clients.http;

import java.io.InputStream;

@FunctionalInterface
public interface DUUIDeserializer<T> {
    T deserialize(InputStream input) throws Exception;
}
