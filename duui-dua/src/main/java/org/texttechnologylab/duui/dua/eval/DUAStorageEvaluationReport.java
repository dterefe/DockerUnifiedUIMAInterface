package org.texttechnologylab.duui.dua.eval;

import java.util.List;

public record DUAStorageEvaluationReport(List<DUAEvaluationResult> results) {
    public DUAStorageEvaluationReport {
        results = results == null ? List.of() : List.copyOf(results);
    }

    public DUAEvaluationResult baseline() {
        return results.stream()
                .filter(result -> result.name().equals("uima-heap-baseline"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Missing uima-heap-baseline result"));
    }

    public String summary() {
        StringBuilder builder = new StringBuilder();
        for (DUAEvaluationResult result : results) {
            builder.append(result.summaryLine()).append(System.lineSeparator());
        }
        return builder.toString();
    }
}
