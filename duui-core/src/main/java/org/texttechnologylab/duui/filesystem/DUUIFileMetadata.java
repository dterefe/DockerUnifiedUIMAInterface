package org.texttechnologylab.duui.filesystem;

import java.nio.file.attribute.FileTime;
import java.util.Map;

public record DUUIFileMetadata(
    String name,
    String path,
    String extension,
    String mediaType,
    long size,
    boolean exists,
    boolean file,
    boolean directory,
    boolean symbolicLink,
    boolean hidden,
    boolean readable,
    boolean writable,
    boolean executable,
    FileTime createdAt,
    FileTime modifiedAt,
    FileTime accessedAt,
    String owner,
    Map<String, Object> attributes
) {
}
