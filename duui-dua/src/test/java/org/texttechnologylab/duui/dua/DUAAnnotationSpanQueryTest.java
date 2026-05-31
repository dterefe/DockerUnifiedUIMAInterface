package org.texttechnologylab.duui.dua;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DUAAnnotationSpanQueryTest {
    @Test
    void spanRecordCapturesMinimalAnnotationIndexRow() {
        DUAAnnotationSpan span = new DUAAnnotationSpan(
                100,
                42,
                7,
                10,
                20,
                Optional.of("Berlin"));

        assertEquals(100, span.sofaFsRef());
        assertEquals(42, span.fsRef());
        assertEquals(7, span.typeId());
        assertEquals(10, span.begin());
        assertEquals(20, span.end());
        assertEquals(Optional.of("Berlin"), span.coveredText());
    }

    @Test
    void spanQueriesModelRangeTextJoinAndPatternSemantics() {
        DUAAnnotationSpanQuery exact = new DUAAnnotationSpanQuery.ExactSpan(
                100, 10, 20, OptionalInt.of(1));
        DUAAnnotationSpanQuery coveringPoint = new DUAAnnotationSpanQuery.CoveringPoint(
                100, 15, OptionalInt.empty());
        DUAAnnotationSpanQuery sameSpanJoin = new DUAAnnotationSpanQuery.SameSpanJoin(
                100, 2, 3);
        DUAAnnotationSpanQuery substring = new DUAAnnotationSpanQuery.Substring(
                100, "Berlin", OptionalInt.of(4));

        DUAAnnotationSpanQuery.Pattern pattern = new DUAAnnotationSpanQuery.Pattern(
                100,
                List.of(exact, coveringPoint, sameSpanJoin, substring));

        assertEquals(4, pattern.steps().size());
    }

    @Test
    void invalidSpansAreRejectedAtApiBoundary() {
        assertThrows(IllegalArgumentException.class,
                () -> new DUAAnnotationSpan(100, 1, 1, 20, 10, Optional.empty()));
        assertThrows(IllegalArgumentException.class,
                () -> new DUAAnnotationSpanQuery.Substring(100, " ", OptionalInt.empty()));
        assertThrows(IllegalArgumentException.class,
                () -> new DUAAnnotationSpanQuery.Pattern(100, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DUAAnnotationSpan(-1, 1, 1, 0, 1, Optional.empty()));
    }
}
