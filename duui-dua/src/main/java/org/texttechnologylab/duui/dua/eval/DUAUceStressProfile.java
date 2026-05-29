package org.texttechnologylab.duui.dua.eval;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public record DUAUceStressProfile(String name,
                                  int documents,
                                  int pagesPerDocument,
                                  int annotationsPerDocument,
                                  int metadataFieldsPerDocument,
                                  int semanticRoleEventsPerDocument,
                                  int associationsPerDocument,
                                  int queryIterations,
                                  int virtualThreads,
                                  int writesPerThread) {
    public static DUAUceStressProfile smoke() {
        return new DUAUceStressProfile("uce-reference-smoke", 24, 4, 120, 8, 16, 80, 3, 32, 128);
    }

    public static DUAUceStressProfile sample1000Approximation() {
        return new DUAUceStressProfile("biofid-sample-1000-reference", 1_000, 6, 350, 16, 48, 240, 5, 256, 1_000);
    }

    public static DUAUceStressProfile biofid24kApproximation() {
        return new DUAUceStressProfile("biofid-24035-reference", 24_035, 6, 350, 16, 48, 240, 3, 512, 1_000);
    }

    public static DUAUceStressProfile fromBiofidSampleManifest(Path manifest) throws IOException {
        long rows = 0;
        long totalBytes = 0;
        long maxBytes = 0;
        try (var lines = Files.lines(manifest)) {
            var iterator = lines.iterator();
            if (iterator.hasNext()) {
                iterator.next();
            }
            while (iterator.hasNext()) {
                String line = iterator.next();
                if (line.isBlank()) {
                    continue;
                }
                String[] columns = line.split("\t");
                if (columns.length < 2) {
                    continue;
                }
                long bytes = Long.parseLong(columns[1]);
                rows++;
                totalBytes += bytes;
                maxBytes = Math.max(maxBytes, bytes);
            }
        }
        if (rows < 1) {
            throw new IllegalArgumentException("Manifest has no sample rows: " + manifest);
        }
        return fromCompressedSizeStats("biofid-manifest-" + rows + "-reference",
                Math.toIntExact(rows), totalBytes / rows, maxBytes);
    }

    public static DUAUceStressProfile fromCompressedSizeStats(String name,
                                                              int documents,
                                                              long meanCompressedBytes,
                                                              long maxCompressedBytes) {
        int pages = Math.max(1, Math.toIntExact(Math.min(64,
                Math.max(1, Math.round(meanCompressedBytes / 75_000.0d)))));
        int annotations = Math.max(50, Math.toIntExact(Math.min(5_000,
                Math.round(meanCompressedBytes / 1_250.0d))));
        int metadata = meanCompressedBytes > 500_000 ? 20 : 16;
        int srlEvents = Math.max(8, annotations / 8);
        int associations = Math.max(annotations / 2, Math.toIntExact(Math.min(4_000,
                Math.round(annotations * 0.70d))));
        int queryIterations = documents > 10_000 || maxCompressedBytes > 2_000_000 ? 3 : 5;
        int virtualThreads = documents > 10_000 ? 512 : 256;
        int writesPerThread = documents > 10_000 ? 1_000 : 750;
        return new DUAUceStressProfile(name, documents, pages, annotations, metadata,
                srlEvents, associations, queryIterations, virtualThreads, writesPerThread);
    }

    public DUAUceStressProfile {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (documents < 1 || pagesPerDocument < 1 || annotationsPerDocument < 1
                || metadataFieldsPerDocument < 1 || semanticRoleEventsPerDocument < 1
                || associationsPerDocument < 1 || queryIterations < 1
                || virtualThreads < 1 || writesPerThread < 1) {
            throw new IllegalArgumentException("all stress profile counts must be positive");
        }
    }

    long pageCount() {
        return documents * (long) pagesPerDocument;
    }

    long annotationCount() {
        return documents * (long) annotationsPerDocument;
    }

    long metadataCount() {
        return documents * (long) metadataFieldsPerDocument;
    }

    long semanticRoleEventCount() {
        return documents * (long) semanticRoleEventsPerDocument;
    }

    long associationCount() {
        return documents * (long) associationsPerDocument;
    }
}
