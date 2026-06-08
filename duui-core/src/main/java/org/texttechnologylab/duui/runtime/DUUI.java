package org.texttechnologylab.duui.runtime;

/**
 * Static entry point for DUUI pipeline construction.
 *
 * <p>Fluent builder API [DESIGN: lines 401-430]:</p>
 * <pre>{@code
 *   DUUI.pipeline("my-pipeline")
 *       .source(mySource)
 *       .checkpoint("checkpoint-1")
 *       .sink(myTarget)
 *       .build()
 * }</pre>
 *
 * <p>For the full scoped builder with orchestration:</p>
 * <pre>{@code
 *   try (DUUISystemScope system = DUUI.system("my-system")) {
 *       DUUIPipelineScope pipeline = system.pipeline("my-pipeline");
 *       // configure pipeline...
 *   }
 * }</pre>
 */
public final class DUUI {
    private DUUI() {
    }

    /**
     * Create a system scope for full orchestration.
     * [DESIGN: lines 105-106]
     */
    public static DUUISystemScope system(String id) {
        return new DUUISystemScope(id);
    }

    /**
     * Shortcut for simple pipeline construction.
     * Returns a DUUIPipelineScope bound to a fresh DUUISystemScope.
     * [DESIGN: lines 401-430]
     */
    public static DUUIPipelineScope pipeline(String id) {
        return new DUUISystemScope(id).pipeline(id);
    }
}
