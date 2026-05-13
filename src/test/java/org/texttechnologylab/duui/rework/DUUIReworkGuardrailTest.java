package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;

class DUUIReworkGuardrailTest {
    @Test
    void migratedCoreDoesNotDependOnShellObservabilityPackages() throws Exception {
        String source = coreSource();

        assertFalse(source.contains("org.texttechnologylab.duui.monitoring"));
        assertFalse(source.contains("DUUIPhaseExecutor"));
        assertFalse(source.contains("PhaseAspect"));
    }

    @Test
    void rawExecutorCreationStaysOutOfNewExecutionPaths() throws Exception {
        String source = coreSource();

        assertFalse(source.contains("Executors.newFixedThreadPool"));
        assertFalse(source.contains("Executors.newCachedThreadPool"));
        assertFalse(source.contains("Executors.newVirtualThreadPerTaskExecutor"));
        assertFalse(source.contains("new Thread("));
    }

    private static String coreSource() throws Exception {
        Path root = Path.of("src/main/java/org/texttechnologylab/duui");
        StringBuilder builder = new StringBuilder();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                builder.append(Files.readString(path)).append('\n');
            }
        }
        return builder.toString();
    }
}
