package org.texttechnologylab.duui.dua.archive;

import org.texttechnologylab.duui.dua.backend.DUAStoreRole;

public record DUAStoreSnapshotEntry(String id, DUAStoreRole role, String path, String mediaType) {
}
