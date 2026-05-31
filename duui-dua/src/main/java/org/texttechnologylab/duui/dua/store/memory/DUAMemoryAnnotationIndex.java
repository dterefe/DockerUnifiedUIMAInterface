package org.texttechnologylab.duui.dua.store.memory;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;
import org.texttechnologylab.duui.dua.store.DUAAnnotationIndex;
import org.texttechnologylab.duui.dua.store.DUARevision;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

public final class DUAMemoryAnnotationIndex implements DUAAnnotationIndex {
    private final CopyOnWriteArrayList<DUAAnnotationSpan> spans = new CopyOnWriteArrayList<>();

    @Override
    public DUAWriteResult index(DUAAnnotationSpan span) {
        spans.add(Objects.requireNonNull(span, "span"));
        return new DUAWriteResult(
                DUAId.of("sofa-" + span.sofaFsRef() + "#ann-" + span.fsRef()),
                new DUARevision(spans.size()));
    }

    @Override
    public Stream<DUAAnnotationSpan> find(DUAAnnotationSpanQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUAAnnotationSpanQuery.ExactSpan q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> span.begin() == q.begin() && span.end() == q.end())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.CoveringPoint q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> span.begin() <= q.offset() && q.offset() < span.end())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.Overlapping q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> span.begin() < q.end() && q.begin() < span.end())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.ContainedIn q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> q.begin() <= span.begin() && span.end() <= q.end())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.CoveringSpan q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> span.begin() <= q.begin() && q.end() <= span.end())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.Neighborhood q -> neighborhood(q);
            case DUAAnnotationSpanQuery.SameSpanJoin q -> spans.stream()
                    .filter(left -> sameSofa(left, q.sofaFsRef()))
                    .filter(left -> left.typeId() == q.leftTypeId())
                    .filter(left -> spans.stream().anyMatch(right ->
                            sameSofa(right, q.sofaFsRef())
                                    && right.typeId() == q.rightTypeId()
                                    && right.begin() == left.begin()
                                    && right.end() == left.end()));
            case DUAAnnotationSpanQuery.RangeJoin q -> spans.stream()
                    .filter(outer -> sameSofa(outer, q.sofaFsRef()))
                    .filter(outer -> outer.typeId() == q.outerTypeId())
                    .filter(outer -> spans.stream().anyMatch(inner ->
                            sameSofa(inner, q.sofaFsRef())
                                    && inner.typeId() == q.innerTypeId()
                                    && outer.begin() <= inner.begin()
                                    && inner.end() <= outer.end()));
            case DUAAnnotationSpanQuery.CoveredText q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> span.coveredText().filter(q.text()::equals).isPresent())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.Substring q -> spans.stream()
                    .filter(span -> sameSofa(span, q.sofaFsRef()))
                    .filter(span -> span.coveredText().filter(text -> text.contains(q.text())).isPresent())
                    .filter(span -> hasType(span, q.typeId()));
            case DUAAnnotationSpanQuery.Pattern q -> q.steps().stream()
                    .flatMap(this::find)
                    .distinct();
        };
    }

    private Stream<DUAAnnotationSpan> neighborhood(DUAAnnotationSpanQuery.Neighborhood query) {
        List<DUAAnnotationSpan> scoped = spans.stream()
                .filter(span -> sameSofa(span, query.sofaFsRef()))
                .sorted((left, right) -> Integer.compare(left.begin(), right.begin()))
                .toList();
        int anchor = -1;
        for (int i = 0; i < scoped.size(); i++) {
            if (scoped.get(i).fsRef() == query.anchorFsRef()) {
                anchor = i;
                break;
            }
        }
        if (anchor < 0) {
            return Stream.empty();
        }
        int from = Math.max(0, anchor - query.before());
        int to = Math.min(scoped.size(), anchor + query.after() + 1);
        return scoped.subList(from, to).stream()
                .filter(span -> hasType(span, query.typeId()));
    }

    private static boolean sameSofa(DUAAnnotationSpan span, long sofaFsRef) {
        return span.sofaFsRef() == sofaFsRef;
    }

    private static boolean hasType(DUAAnnotationSpan span, OptionalInt typeId) {
        return typeId.isEmpty() || span.typeId() == typeId.getAsInt();
    }
}
