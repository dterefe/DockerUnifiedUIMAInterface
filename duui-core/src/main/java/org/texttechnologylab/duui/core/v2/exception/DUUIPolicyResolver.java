package org.texttechnologylab.duui.exception;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public final class DUUIPolicyResolver {
    private final DUUIFailurePolicy defaultPolicy;

    public DUUIPolicyResolver(DUUIFailurePolicy defaultPolicy) {
        this.defaultPolicy = defaultPolicy == null ? DUUIFailurePolicy.FAIL_FAST : defaultPolicy;
    }

    public DUUIFailurePolicy resolve(DUUIPipeline pipeline, DUUICheckpoint<?> checkpoint, DUUIStage<?> stage) {
        if (stage != null && stage.failurePolicy() != null) return stage.failurePolicy();
        if (checkpoint != null && checkpoint.failurePolicy() != null) return checkpoint.failurePolicy();
        if (pipeline != null && pipeline.failurePolicy() != null) return pipeline.failurePolicy();
        return defaultPolicy;
    }
}
