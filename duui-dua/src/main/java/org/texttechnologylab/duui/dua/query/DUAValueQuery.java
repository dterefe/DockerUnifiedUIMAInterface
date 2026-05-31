package org.texttechnologylab.duui.dua.query;

import java.util.Objects;
import java.util.OptionalInt;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;

public sealed interface DUAValueQuery permits
        DUAValueQuery.FeatureEquals,
        DUAValueQuery.FeatureRange,
        DUAValueQuery.ReferenceTarget,
        DUAValueQuery.CollectionContains {

    record FeatureEquals(DUAId casId, DUAId viewId, OptionalInt typeId, String featureName, DUACasValue value)
            implements DUAValueQuery {
        public FeatureEquals {
            Objects.requireNonNull(casId, "casId");
            Objects.requireNonNull(viewId, "viewId");
            Objects.requireNonNull(typeId, "typeId");
            Objects.requireNonNull(featureName, "featureName");
            Objects.requireNonNull(value, "value");
        }
    }

    record FeatureRange(DUAId casId, DUAId viewId, OptionalInt typeId, String featureName,
                        long lowerInclusive, long upperInclusive) implements DUAValueQuery {
        public FeatureRange {
            Objects.requireNonNull(casId, "casId");
            Objects.requireNonNull(viewId, "viewId");
            Objects.requireNonNull(typeId, "typeId");
            Objects.requireNonNull(featureName, "featureName");
            if (upperInclusive < lowerInclusive) {
                throw new IllegalArgumentException("upperInclusive must be greater than or equal to lowerInclusive");
            }
        }
    }

    record ReferenceTarget(DUAId casId, DUAId viewId, String featureName, long targetFsRef)
            implements DUAValueQuery {
        public ReferenceTarget {
            Objects.requireNonNull(casId, "casId");
            Objects.requireNonNull(viewId, "viewId");
            Objects.requireNonNull(featureName, "featureName");
        }
    }

    record CollectionContains(DUAId casId, DUAId viewId, long collectionFsRef, DUACasValue value)
            implements DUAValueQuery {
        public CollectionContains {
            Objects.requireNonNull(casId, "casId");
            Objects.requireNonNull(viewId, "viewId");
            Objects.requireNonNull(value, "value");
        }
    }
}
