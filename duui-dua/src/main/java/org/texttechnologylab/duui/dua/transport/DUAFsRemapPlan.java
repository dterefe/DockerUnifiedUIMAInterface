package org.texttechnologylab.duui.dua.transport;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record DUAFsRemapPlan(String sourceNamespace,
                             String targetNamespace,
                             DUAFsIdentityMode identityMode,
                             List<DUAFsIdMapEntry> entries) {
    public DUAFsRemapPlan {
        identityMode = identityMode == null ? DUAFsIdentityMode.XMI_LOCAL : identityMode;
        entries = entries == null ? List.of() : List.copyOf(entries);
        if (identityMode == DUAFsIdentityMode.EXPLICIT_REMAP && entries.isEmpty()) {
            throw new IllegalArgumentException("explicit-remap requires at least one FS id mapping");
        }
        if (identityMode != DUAFsIdentityMode.XMI_LOCAL
                && (targetNamespace == null || targetNamespace.isBlank())) {
            throw new IllegalArgumentException(identityMode.wireName() + " requires targetNamespace");
        }
    }

    public static DUAFsRemapPlan xmiLocal() {
        return new DUAFsRemapPlan(null, null, DUAFsIdentityMode.XMI_LOCAL, List.of());
    }

    public static DUAFsRemapPlan global(String sourceNamespace, String targetNamespace) {
        return new DUAFsRemapPlan(sourceNamespace, targetNamespace, DUAFsIdentityMode.STABLE_GLOBAL_GID, List.of());
    }

    public static DUAFsRemapPlan explicit(String sourceNamespace,
                                          String targetNamespace,
                                          List<DUAFsIdMapEntry> entries) {
        return new DUAFsRemapPlan(sourceNamespace, targetNamespace, DUAFsIdentityMode.EXPLICIT_REMAP, entries);
    }

    public String targetFor(String sourceFsId) {
        if (identityMode == DUAFsIdentityMode.XMI_LOCAL) {
            throw new DUADocumentTransferException("XMI-local transfers do not preserve source FS ids");
        }
        if (identityMode == DUAFsIdentityMode.STABLE_GLOBAL_GID) {
            return targetNamespace + ":" + sourceFsId;
        }
        return explicitMap().get(sourceFsId);
    }

    public Map<String, String> explicitMap() {
        Map<String, String> mapping = new LinkedHashMap<>();
        for (DUAFsIdMapEntry entry : entries) {
            mapping.put(entry.sourceFsId(), entry.targetFsId());
        }
        return Map.copyOf(mapping);
    }
}
