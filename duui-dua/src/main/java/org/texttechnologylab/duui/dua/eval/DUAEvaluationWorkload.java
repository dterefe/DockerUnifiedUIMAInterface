package org.texttechnologylab.duui.dua.eval;

public record DUAEvaluationWorkload(int featureStructures, int readIterations,
                                    int virtualThreads, int writesPerThread) {
    public static DUAEvaluationWorkload smoke() {
        return new DUAEvaluationWorkload(2_000, 20, 64, 200);
    }

    public static DUAEvaluationWorkload standard() {
        return new DUAEvaluationWorkload(50_000, 25, 256, 1_000);
    }

    public DUAEvaluationWorkload {
        if (featureStructures < 1) {
            throw new IllegalArgumentException("featureStructures must be positive");
        }
        if (readIterations < 1) {
            throw new IllegalArgumentException("readIterations must be positive");
        }
        if (virtualThreads < 1) {
            throw new IllegalArgumentException("virtualThreads must be positive");
        }
        if (writesPerThread < 1) {
            throw new IllegalArgumentException("writesPerThread must be positive");
        }
    }
}
