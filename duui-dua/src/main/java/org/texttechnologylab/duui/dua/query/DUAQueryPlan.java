package org.texttechnologylab.duui.dua.query;

import java.util.List;
import java.util.Objects;

public record DUAQueryPlan(List<DUAQueryPlanStep> steps) {
    public DUAQueryPlan {
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    public static DUAQueryPlan of(DUAQueryPlanStep... steps) {
        return new DUAQueryPlan(List.of(steps));
    }

    public record DUAQueryPlanStep(String storeName, String operation, DUAQuery query) {
        public DUAQueryPlanStep {
            Objects.requireNonNull(storeName, "storeName");
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(query, "query");
        }
    }
}
