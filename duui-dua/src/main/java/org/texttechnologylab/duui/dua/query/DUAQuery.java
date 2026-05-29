package org.texttechnologylab.duui.dua.query;

import java.util.List;
import java.util.Objects;
import org.texttechnologylab.duui.dua.model.DUAAssociationType;
import org.texttechnologylab.duui.dua.model.DUAEntityKind;
import org.texttechnologylab.duui.dua.model.DUAFeatureKey;
import org.texttechnologylab.duui.dua.model.DUAScope;
import org.texttechnologylab.duui.dua.model.DUAValue;

public sealed interface DUAQuery permits DUAQuery.MatchAll, DUAQuery.EntityKindMatch,
        DUAQuery.ScopeMatch, DUAQuery.FeaturePredicate, DUAQuery.AssociationTraversal,
        DUAQuery.And, DUAQuery.Or, DUAQuery.Not, DUAQuery.OrderedNear {
    record MatchAll() implements DUAQuery {
    }

    record EntityKindMatch(DUAEntityKind kind) implements DUAQuery {
        public EntityKindMatch {
            Objects.requireNonNull(kind, "kind");
        }
    }

    record ScopeMatch(DUAScope scope) implements DUAQuery {
        public ScopeMatch {
            Objects.requireNonNull(scope, "scope");
        }
    }

    record FeaturePredicate(DUAFeatureKey feature, DUAComparisonOperator operator, DUAValue value) implements DUAQuery {
        public FeaturePredicate {
            Objects.requireNonNull(feature, "feature");
            Objects.requireNonNull(operator, "operator");
        }
    }

    record AssociationTraversal(DUAAssociationType type, DUAQuery source, DUAQuery target) implements DUAQuery {
        public AssociationTraversal {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
        }
    }

    record And(List<DUAQuery> children) implements DUAQuery {
        public And {
            children = children == null ? List.of() : List.copyOf(children);
        }
    }

    record Or(List<DUAQuery> children) implements DUAQuery {
        public Or {
            children = children == null ? List.of() : List.copyOf(children);
        }
    }

    record Not(DUAQuery child) implements DUAQuery {
        public Not {
            Objects.requireNonNull(child, "child");
        }
    }

    record OrderedNear(DUAQuery left, DUAQuery right, int maxDistance) implements DUAQuery {
        public OrderedNear {
            Objects.requireNonNull(left, "left");
            Objects.requireNonNull(right, "right");
            if (maxDistance < 1) {
                throw new IllegalArgumentException("maxDistance must be positive");
            }
        }
    }
}
