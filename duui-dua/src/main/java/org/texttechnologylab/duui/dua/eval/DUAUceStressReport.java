package org.texttechnologylab.duui.dua.eval;

import java.util.List;

public record DUAUceStressReport(List<DUAUceStressResult> results) {
    public DUAUceStressReport {
        results = results == null ? List.of() : List.copyOf(results);
    }

    public String summary() {
        StringBuilder builder = new StringBuilder();
        for (DUAUceStressResult result : results) {
            builder.append(result.summaryLine()).append(System.lineSeparator());
        }
        return builder.toString();
    }

    public String detailedSummary() {
        StringBuilder builder = new StringBuilder();
        for (DUAUceStressResult result : results) {
            builder.append(result.detailedSummaryLine()).append(System.lineSeparator());
        }
        return builder.toString();
    }
}
