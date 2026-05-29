package org.texttechnologylab.duui.dua.eval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DUAStreamingCasEvaluatorTest {
    @Test
    void streamingEvaluationMaterializesDocumentsWithoutRetainingJCasHeap() throws Exception {
        DUAStreamingEvaluationResult dense = DUAStreamingCasEvaluator.evaluateDenseEphemeral(24, 200);
        DUAStreamingEvaluationResult sqlite = DUAStreamingCasEvaluator.evaluateSqlitePersistent(24, 200);

        assertEquals(dense.checksum(), sqlite.checksum());
        assertTrue(dense.maxLiveMemoryDeltaBytes() < 64L * 1024L * 1024L, dense.summaryLine());
        assertTrue(sqlite.maxLiveMemoryDeltaBytes() < 64L * 1024L * 1024L, sqlite.summaryLine());
    }
}
