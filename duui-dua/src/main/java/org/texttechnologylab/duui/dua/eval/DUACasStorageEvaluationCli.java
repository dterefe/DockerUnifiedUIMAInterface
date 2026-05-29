package org.texttechnologylab.duui.dua.eval;

import java.nio.file.Files;
import java.nio.file.Path;

public final class DUACasStorageEvaluationCli {
    private DUACasStorageEvaluationCli() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equalsIgnoreCase("uce-stress")) {
            DUAUceStressProfile profile = resolveUceStressProfile(args);
            boolean includeSqlite = args.length <= 2 || !args[2].equalsIgnoreCase("memory-only");
            System.out.println(DUAUceBackendStressEvaluator.evaluate(profile, includeSqlite).detailedSummary());
            return;
        }
        if (args.length > 0 && args[0].equalsIgnoreCase("stream")) {
            int documents = args.length > 1 ? Integer.parseInt(args[1]) : 64;
            int annotationsPerDocument = args.length > 2 ? Integer.parseInt(args[2]) : 500;
            System.out.println(DUAStreamingCasEvaluator
                    .evaluateDenseEphemeral(documents, annotationsPerDocument)
                    .summaryLine());
            System.out.println(DUAStreamingCasEvaluator
                    .evaluateSqlitePersistent(documents, annotationsPerDocument)
                    .summaryLine());
            return;
        }
        DUAEvaluationWorkload workload = args.length > 0 && args[0].equalsIgnoreCase("standard")
                ? DUAEvaluationWorkload.standard()
                : DUAEvaluationWorkload.smoke();
        DUAStorageEvaluationReport report = DUACasStorageEvaluator.evaluate(workload, true);
        DUAEvaluationResult baseline = report.baseline();
        System.out.println(report.summary());
        for (DUAEvaluationResult result : report.results()) {
            if (result == baseline) {
                continue;
            }
            double readRatio = result.readOpsPerSecond() / baseline.readOpsPerSecond();
            double writeRatio = result.writeOpsPerSecond() / baseline.writeOpsPerSecond();
            double concurrentRatio = result.concurrentWriteOpsPerSecond() / baseline.concurrentWriteOpsPerSecond();
            System.out.printf("%s ratios vs heap: write=%.3f read=%.3f vt-write=%.3f%n",
                    result.name(), writeRatio, readRatio, concurrentRatio);
        }
    }

    private static DUAUceStressProfile resolveUceStressProfile(String[] args) throws Exception {
        if (args.length <= 1 || args[1].equalsIgnoreCase("smoke")) {
            return DUAUceStressProfile.smoke();
        }
        if (args[1].equalsIgnoreCase("sample1000")) {
            return DUAUceStressProfile.sample1000Approximation();
        }
        if (args[1].equalsIgnoreCase("biofid24k")) {
            return DUAUceStressProfile.biofid24kApproximation();
        }
        Path manifest = Path.of(args[1]);
        if (Files.exists(manifest)) {
            return DUAUceStressProfile.fromBiofidSampleManifest(manifest);
        }
        throw new IllegalArgumentException("Unknown uce-stress profile or manifest path: " + args[1]);
    }
}
