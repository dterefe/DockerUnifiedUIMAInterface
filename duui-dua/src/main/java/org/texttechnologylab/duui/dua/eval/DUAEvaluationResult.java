package org.texttechnologylab.duui.dua.eval;

import java.util.Locale;

public record DUAEvaluationResult(String name,
                                  DUAEvaluationWorkload workload,
                                  long writeNanos,
                                  long readNanos,
                                  long concurrentWriteNanos,
                                  long memoryDeltaBytes,
                                  long checksum) {
    public long writeOps() {
        return workload.featureStructures() * 2L;
    }

    public long readOps() {
        return workload.featureStructures() * workload.readIterations() * 2L;
    }

    public long concurrentWriteOps() {
        return workload.virtualThreads() * (long) workload.writesPerThread();
    }

    public double writeOpsPerSecond() {
        return perSecond(writeOps(), writeNanos);
    }

    public double readOpsPerSecond() {
        return perSecond(readOps(), readNanos);
    }

    public double concurrentWriteOpsPerSecond() {
        return perSecond(concurrentWriteOps(), concurrentWriteNanos);
    }

    public String summaryLine() {
        return String.format(Locale.ROOT,
                "%s write=%.0f ops/s read=%.0f ops/s vt-write=%.0f ops/s memDelta=%d checksum=%d",
                name, writeOpsPerSecond(), readOpsPerSecond(), concurrentWriteOpsPerSecond(),
                memoryDeltaBytes, checksum);
    }

    private static double perSecond(long operations, long nanos) {
        if (nanos <= 0) {
            return Double.POSITIVE_INFINITY;
        }
        return operations / (nanos / 1_000_000_000.0d);
    }
}
