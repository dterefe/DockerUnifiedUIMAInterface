package org.texttechnologylab.duui.dua.transport;

import java.util.ArrayList;
import java.util.List;

public final class DUAFsIdentityPlanner {
    private DUAFsIdentityPlanner() {
    }

    public static DUAFsRemapPlan forDocument(DUADocumentTransferManifest manifest,
                                             DUADocumentTransferEntry document,
                                             String targetNamespace) {
        DUAFsIdentityMode mode = DUAFsIdentityMode.fromWireName(manifest.fsIdentityMode());
        String sourceNamespace = document.sourceFsNamespace();
        String effectiveTargetNamespace = document.targetFsNamespace() == null || document.targetFsNamespace().isBlank()
                ? targetNamespace
                : document.targetFsNamespace();
        return switch (mode) {
            case XMI_LOCAL -> DUAFsRemapPlan.xmiLocal();
            case STABLE_GLOBAL_GID -> DUAFsRemapPlan.global(sourceNamespace, requireTarget(effectiveTargetNamespace, mode));
            case EXPLICIT_REMAP -> DUAFsRemapPlan.explicit(
                    sourceNamespace,
                    requireTarget(effectiveTargetNamespace, mode),
                    document.fsIdMap());
        };
    }

    public static DUAFsRemapPlan allocateSequential(String sourceNamespace,
                                                    String targetNamespace,
                                                    String viewName,
                                                    String typeName,
                                                    List<Long> sourceFsRefs,
                                                    long firstTargetRef) {
        if (sourceFsRefs == null || sourceFsRefs.isEmpty()) {
            throw new IllegalArgumentException("sourceFsRefs must not be empty");
        }
        List<DUAFsIdMapEntry> entries = new ArrayList<>(sourceFsRefs.size());
        long next = firstTargetRef;
        for (Long source : sourceFsRefs) {
            if (source == null || source < 1) {
                throw new IllegalArgumentException("source FS refs must be positive");
            }
            entries.add(new DUAFsIdMapEntry(
                    sourceNamespace + ":" + source,
                    targetNamespace + ":" + next++,
                    typeName,
                    viewName));
        }
        return DUAFsRemapPlan.explicit(sourceNamespace, targetNamespace, entries);
    }

    private static String requireTarget(String targetNamespace, DUAFsIdentityMode mode) {
        if (targetNamespace == null || targetNamespace.isBlank()) {
            throw new DUADocumentTransferException(mode.wireName() + " requires a target FS namespace");
        }
        return targetNamespace;
    }
}
