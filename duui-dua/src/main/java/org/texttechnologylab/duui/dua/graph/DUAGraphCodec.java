package org.texttechnologylab.duui.dua.graph;

import java.io.IOException;
import java.nio.file.Path;

public interface DUAGraphCodec {
    String id();

    String defaultFileName();

    void write(DUAGraphPartition partition, Path target) throws IOException;

    DUAGraphPartition read(String partitionId, String scope, Path source) throws IOException;
}
