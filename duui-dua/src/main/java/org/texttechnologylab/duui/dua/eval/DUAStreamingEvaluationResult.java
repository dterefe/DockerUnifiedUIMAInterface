package org.texttechnologylab.duui.dua.eval;

import java.util.Locale;

public record DUAStreamingEvaluationResult(String name,
                                           int documents,
                                           int annotationsPerDocument,
                                           long maxLiveMemoryDeltaBytes,
                                           long finalMemoryDeltaBytes,
                                           long checksum) {
    public String summaryLine() {
        return String.format(Locale.ROOT,
                "%s docs=%d annotations/doc=%d maxLiveDelta=%d finalDelta=%d checksum=%d",
                name, documents, annotationsPerDocument, maxLiveMemoryDeltaBytes, finalMemoryDeltaBytes, checksum);
    }
}
