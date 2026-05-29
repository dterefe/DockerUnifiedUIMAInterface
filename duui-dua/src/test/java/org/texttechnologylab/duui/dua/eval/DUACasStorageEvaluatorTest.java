package org.texttechnologylab.duui.dua.eval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DUACasStorageEvaluatorTest {
    @Test
    void smokeEvaluationComparesBackendsAgainstHeapBaseline() throws Exception {
        DUAStorageEvaluationReport report = DUACasStorageEvaluator.evaluateSmoke();

        assertEquals(8, report.results().size(), report.summary());
        long baselineChecksum = report.baseline().checksum();
        for (DUAEvaluationResult result : report.results()) {
            assertEquals(baselineChecksum, result.checksum(), result.summaryLine());
            assertTrue(result.readOpsPerSecond() > 0, result.summaryLine());
            assertTrue(result.writeOpsPerSecond() > 0, result.summaryLine());
            assertTrue(result.concurrentWriteOpsPerSecond() > 0, result.summaryLine());
        }
    }
}
