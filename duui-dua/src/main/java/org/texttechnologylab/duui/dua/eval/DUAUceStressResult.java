package org.texttechnologylab.duui.dua.eval;

import java.util.Locale;

public record DUAUceStressResult(String backendName,
                                 DUAUceStressProfile profile,
                                 long ingestNanos,
                                 long fulltextNanos,
                                 long metadataNanos,
                                 long semanticRoleNanos,
                                 long annotationSummaryNanos,
                                 long associationNanos,
                                 long concurrentWriteNanos,
                                 long memoryDeltaBytes,
                                 long checksum) {
    public double ingestOpsPerSecond() {
        return perSecond(ingestOps(), ingestNanos);
    }

    public double queryOpsPerSecond() {
        return perSecond(queryOps(), fulltextNanos + metadataNanos + semanticRoleNanos
                + annotationSummaryNanos + associationNanos);
    }

    public double fulltextOpsPerSecond() {
        return perSecond(fulltextOps(), fulltextNanos);
    }

    public double metadataOpsPerSecond() {
        return perSecond(metadataOps(), metadataNanos);
    }

    public double semanticRoleOpsPerSecond() {
        return perSecond(semanticRoleOps(), semanticRoleNanos);
    }

    public double annotationSummaryOpsPerSecond() {
        return perSecond(annotationSummaryOps(), annotationSummaryNanos);
    }

    public double associationOpsPerSecond() {
        return perSecond(associationOps(), associationNanos);
    }

    public double concurrentWriteOpsPerSecond() {
        return perSecond(concurrentWriteOps(), concurrentWriteNanos);
    }

    public long ingestOps() {
        return profile.pageCount()
                + profile.annotationCount()
                + profile.metadataCount()
                + profile.semanticRoleEventCount()
                + profile.associationCount();
    }

    public long queryOps() {
        return fulltextOps() + metadataOps() + semanticRoleOps() + annotationSummaryOps() + associationOps();
    }

    public long fulltextOps() {
        return profile.pageCount() * profile.queryIterations();
    }

    public long metadataOps() {
        return profile.metadataCount() * profile.queryIterations();
    }

    public long semanticRoleOps() {
        return profile.semanticRoleEventCount() * profile.queryIterations();
    }

    public long annotationSummaryOps() {
        return profile.annotationCount() * profile.queryIterations();
    }

    public long associationOps() {
        return profile.associationCount() * profile.queryIterations();
    }

    public long concurrentWriteOps() {
        return profile.virtualThreads() * (long) profile.writesPerThread();
    }

    public String summaryLine() {
        return String.format(Locale.ROOT,
                "%s profile=%s ingest=%.0f ops/s query=%.0f ops/s vt-write=%.0f ops/s memDelta=%d checksum=%d",
                backendName, profile.name(), ingestOpsPerSecond(), queryOpsPerSecond(),
                concurrentWriteOpsPerSecond(), memoryDeltaBytes, checksum);
    }

    public String detailedSummaryLine() {
        return String.format(Locale.ROOT,
                "%s profile=%s ingest=%.0f fulltext=%.0f metadata=%.0f srl=%.0f annotations=%.0f associations=%.0f vt-write=%.0f memDelta=%d checksum=%d",
                backendName, profile.name(), ingestOpsPerSecond(), fulltextOpsPerSecond(),
                metadataOpsPerSecond(), semanticRoleOpsPerSecond(), annotationSummaryOpsPerSecond(),
                associationOpsPerSecond(), concurrentWriteOpsPerSecond(), memoryDeltaBytes, checksum);
    }

    private static double perSecond(long operations, long nanos) {
        if (nanos <= 0) {
            return Double.POSITIVE_INFINITY;
        }
        return operations / (nanos / 1_000_000_000.0d);
    }
}
