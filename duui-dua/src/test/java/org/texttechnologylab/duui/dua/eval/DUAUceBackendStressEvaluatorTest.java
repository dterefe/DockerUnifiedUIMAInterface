package org.texttechnologylab.duui.dua.eval;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUAUceBackendStressEvaluatorTest {
    @TempDir
    Path temp;

    @Test
    void uceReferenceStressComparesBackendsWithStableChecksums() throws Exception {
        DUAUceStressReport report = DUAUceBackendStressEvaluator.evaluateSmoke();

        assertEquals(6, report.results().size(), report.summary());
        long checksum = report.results().get(0).checksum();
        for (DUAUceStressResult result : report.results()) {
            assertEquals(checksum, result.checksum(), result.summaryLine());
            assertTrue(result.ingestOpsPerSecond() > 0, result.summaryLine());
            assertTrue(result.queryOpsPerSecond() > 0, result.summaryLine());
            assertTrue(result.concurrentWriteOpsPerSecond() > 0, result.summaryLine());
        }
    }

    @Test
    void biofidManifestCanDriveStressProfile() throws Exception {
        Path manifest = temp.resolve("sample_manifest.tsv");
        Files.writeString(manifest, """
                relative_path\tsize_bytes
                a.xmi.gz\t177096
                b.xmi.gz\t388282
                c.xmi.gz\t2040700
                """);

        DUAUceStressProfile profile = DUAUceStressProfile.fromBiofidSampleManifest(manifest);

        assertEquals(3, profile.documents());
        assertTrue(profile.pagesPerDocument() > 1);
        assertTrue(profile.annotationsPerDocument() > 100);
        assertTrue(profile.associationsPerDocument() >= profile.annotationsPerDocument() / 2);
    }
}
