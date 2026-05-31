package org.texttechnologylab.duui.dua.query;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

public sealed interface DUAAnnotationSpanQuery permits
        DUAAnnotationSpanQuery.ExactSpan,
        DUAAnnotationSpanQuery.CoveringPoint,
        DUAAnnotationSpanQuery.Overlapping,
        DUAAnnotationSpanQuery.ContainedIn,
        DUAAnnotationSpanQuery.CoveringSpan,
        DUAAnnotationSpanQuery.Neighborhood,
        DUAAnnotationSpanQuery.SameSpanJoin,
        DUAAnnotationSpanQuery.RangeJoin,
        DUAAnnotationSpanQuery.CoveredText,
        DUAAnnotationSpanQuery.Substring,
        DUAAnnotationSpanQuery.Pattern {

    record ExactSpan(long sofaFsRef, int begin, int end, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public ExactSpan {
            requireSofa(sofaFsRef);
            requireSpan(begin, end);
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record CoveringPoint(long sofaFsRef, int offset, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public CoveringPoint {
            requireSofa(sofaFsRef);
            if (offset < 0) {
                throw new IllegalArgumentException("offset must not be negative");
            }
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record Overlapping(long sofaFsRef, int begin, int end, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public Overlapping {
            requireSofa(sofaFsRef);
            requireSpan(begin, end);
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record ContainedIn(long sofaFsRef, int begin, int end, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public ContainedIn {
            requireSofa(sofaFsRef);
            requireSpan(begin, end);
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record CoveringSpan(long sofaFsRef, int begin, int end, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public CoveringSpan {
            requireSofa(sofaFsRef);
            requireSpan(begin, end);
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record Neighborhood(long sofaFsRef, long anchorFsRef, int before, int after, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public Neighborhood {
            requireSofa(sofaFsRef);
            if (anchorFsRef < 0) {
                throw new IllegalArgumentException("anchorFsRef must not be negative");
            }
            if (before < 0 || after < 0) {
                throw new IllegalArgumentException("before and after must not be negative");
            }
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record SameSpanJoin(long sofaFsRef, int leftTypeId, int rightTypeId)
            implements DUAAnnotationSpanQuery {
        public SameSpanJoin {
            requireSofa(sofaFsRef);
            requireType(leftTypeId);
            requireType(rightTypeId);
        }
    }

    record RangeJoin(long sofaFsRef, int outerTypeId, int innerTypeId)
            implements DUAAnnotationSpanQuery {
        public RangeJoin {
            requireSofa(sofaFsRef);
            requireType(outerTypeId);
            requireType(innerTypeId);
        }
    }

    record CoveredText(long sofaFsRef, String text, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public CoveredText {
            requireSofa(sofaFsRef);
            requireText(text);
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record Substring(long sofaFsRef, String text, OptionalInt typeId)
            implements DUAAnnotationSpanQuery {
        public Substring {
            requireSofa(sofaFsRef);
            requireText(text);
            typeId = typeId == null ? OptionalInt.empty() : typeId;
        }
    }

    record Pattern(long sofaFsRef, List<DUAAnnotationSpanQuery> steps)
            implements DUAAnnotationSpanQuery {
        public Pattern {
            requireSofa(sofaFsRef);
            steps = steps == null ? List.of() : List.copyOf(steps);
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("pattern steps must not be empty");
            }
        }
    }

    private static void requireSofa(long sofaFsRef) {
        if (sofaFsRef < 0) {
            throw new IllegalArgumentException("sofaFsRef must not be negative");
        }
    }

    private static void requireSpan(int begin, int end) {
        if (begin < 0) {
            throw new IllegalArgumentException("begin must not be negative");
        }
        if (end < begin) {
            throw new IllegalArgumentException("end must be greater than or equal to begin");
        }
    }

    private static void requireType(int typeId) {
        if (typeId < 0) {
            throw new IllegalArgumentException("typeId must not be negative");
        }
    }

    private static void requireText(String text) {
        Objects.requireNonNull(text, "text");
        if (text.isBlank()) {
            throw new IllegalArgumentException("text must not be blank");
        }
    }
}
