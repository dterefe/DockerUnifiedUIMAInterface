package org.texttechnologylab.duui.pipeline.io.document;

import org.texttechnologylab.duui.filesystem.DUUIFile;

import java.io.InputStream;

@FunctionalInterface
public interface DUUIDocumentDeserializer<T> {
    T read(DUUIFile source, InputStream input) throws Exception;
}
