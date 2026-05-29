package org.texttechnologylab.duui.dua.pipeline;

import java.nio.file.Path;

public record DUADocumentRef(Path archive, String documentId) {
}
