package org.texttechnologylab.duui.dua.store.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.query.DUATextQuery;
import org.texttechnologylab.duui.dua.store.DUATextQueryStore;
import org.texttechnologylab.duui.dua.store.DUATextRow;

/**
 * In-memory implementation of {@link DUATextQueryStore}.
 * <p>
 * Provides SoFA (Subject of Analysis) management with concurrent-safe storage:
 * <ul>
 *   <li>SoFA registration and lookup by fsRef or local name</li>
 *   <li>Covered text annotation storage</li>
 *   <li>Text queries: Exact, Substring, CoveredText</li>
 * </ul>
 */
public final class DUAMemoryTextQueryStore implements DUATextQueryStore {

    private final AtomicLong nextSofaRef = new AtomicLong(1);
    private final ConcurrentHashMap<Long, DUASofa> sofas = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<DUASofa>> sofasByLocalName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CopyOnWriteArrayList<DUATextRow>> textRows = new ConcurrentHashMap<>();

    // ========== SoFA Management ==========

    /**
     * Creates a new SoFA with the given parameters and registers it.
     *
     * @param sofaId   the sofa ID
     * @param localName the local name
     * @param type     the sofa type
     * @return the newly allocated sofa fsRef
     */
    public long registerSofa(String sofaId, String localName, SofaType type) {
        Objects.requireNonNull(sofaId, "sofaId");
        Objects.requireNonNull(localName, "localName");
        Objects.requireNonNull(type, "type");
        long fsRef = nextSofaRef.getAndIncrement();
        DUASofa sofa = new DUASofa(fsRef, sofaId, localName, new byte[0], type, System.currentTimeMillis());
        sofas.put(fsRef, sofa);
        sofasByLocalName.computeIfAbsent(localName, k -> new CopyOnWriteArrayList<>()).add(sofa);
        return fsRef;
    }

    /**
     * Registers an externally created SoFA.
     *
     * @param sofa the sofa to register
     */
    public void registerSofa(DUASofa sofa) {
        Objects.requireNonNull(sofa, "sofa");
        sofas.put(sofa.fsRef(), sofa);
        sofasByLocalName.computeIfAbsent(sofa.localName(), k -> new CopyOnWriteArrayList<>()).add(sofa);
    }

    /**
     * Retrieves a SoFA by its fsRef.
     *
     * @param sofaFsRef the sofa feature structure reference
     * @return the sofa, or {@code null} if not found
     */
    public DUASofa getSofa(long sofaFsRef) {
        return sofas.get(sofaFsRef);
    }

    /**
     * Finds all SoFAs with the given local name.
     *
     * @param localName the local name to search for
     * @return stream of matching SoFAs
     */
    public Stream<DUASofa> getSofasByLocalName(String localName) {
        Objects.requireNonNull(localName, "localName");
        CopyOnWriteArrayList<DUASofa> matches = sofasByLocalName.get(localName);
        return matches == null ? Stream.empty() : matches.stream();
    }

    /**
     * Returns a stream of all registered SoFAs.
     *
     * @return stream of all SoFAs
     */
    public Stream<DUASofa> getAllSofas() {
        return sofas.values().stream();
    }

    /**
     * Deletes a SoFA and all its associated text rows.
     *
     * @param sofaFsRef the sofa feature structure reference
     */
    public void deleteSofa(long sofaFsRef) {
        DUASofa sofa = sofas.remove(sofaFsRef);
        if (sofa != null) {
            CopyOnWriteArrayList<DUASofa> nameList = sofasByLocalName.get(sofa.localName());
            if (nameList != null) {
                nameList.remove(sofa);
                if (nameList.isEmpty()) {
                    sofasByLocalName.remove(sofa.localName());
                }
            }
        }
        textRows.remove(sofaFsRef);
    }

    /**
     * Adds a covered text annotation for the given SoFA.
     *
     * @param sofaRef       the sofa reference
     * @param annotationRef the annotation reference
     * @param begin         the begin offset
     * @param end           the end offset
     * @param text          the covered text
     */
    public void addCoveredText(long sofaRef, long annotationRef, long begin, long end, String text) {
        if (sofaRef < 0) {
            throw new IllegalArgumentException("sofaRef must not be negative");
        }
        if (annotationRef < 0) {
            throw new IllegalArgumentException("annotationRef must not be negative");
        }
        if (begin < 0 || end < begin) {
            throw new IllegalArgumentException("invalid span: begin=" + begin + ", end=" + end);
        }
        Objects.requireNonNull(text, "text");
        DUATextRow row = new DUATextRow(sofaRef, annotationRef, "coveredText", text);
        textRows.computeIfAbsent(sofaRef, k -> new CopyOnWriteArrayList<>()).add(row);
    }

    /**
     * Adds multiple covered text annotations in bulk.
     *
     * @param rows the rows to add
     */
    public void bulkAddCoveredText(List<DUATextRow> rows) {
        Objects.requireNonNull(rows, "rows");
        for (DUATextRow row : rows) {
            textRows.computeIfAbsent(row.sofaFsRef(), k -> new CopyOnWriteArrayList<>()).add(row);
        }
    }

    /**
     * Removes a covered text annotation.
     *
     * @param sofaRef       the sofa reference
     * @param annotationRef the annotation reference
     */
    public void removeCoveredText(long sofaRef, long annotationRef) {
        CopyOnWriteArrayList<DUATextRow> rows = textRows.get(sofaRef);
        if (rows != null) {
            rows.removeIf(row -> row.fsRef() == annotationRef);
        }
    }

    // ========== Query ==========

    @Override
    public Stream<DUATextRow> find(DUATextQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUATextQuery.Exact q -> findByExact(q);
            case DUATextQuery.Substring q -> findBySubstring(q);
            case DUATextQuery.CoveredText q -> findByCoveredText(q);
        };
    }

    private Stream<DUATextRow> findByExact(DUATextQuery.Exact query) {
        CopyOnWriteArrayList<DUATextRow> rows = textRows.get(query.sofaFsRef());
        if (rows == null) {
            return Stream.empty();
        }
        return rows.stream()
                .filter(row -> query.text().equals(row.text()));
    }

    private Stream<DUATextRow> findBySubstring(DUATextQuery.Substring query) {
        CopyOnWriteArrayList<DUATextRow> rows = textRows.get(query.sofaFsRef());
        if (rows == null) {
            return Stream.empty();
        }
        return rows.stream()
                .filter(row -> row.text() != null && row.text().contains(query.text()));
    }

    private Stream<DUATextRow> findByCoveredText(DUATextQuery.CoveredText query) {
        CopyOnWriteArrayList<DUATextRow> rows = textRows.get(query.sofaFsRef());
        if (rows == null) {
            return Stream.empty();
        }
        Stream<DUATextRow> stream = rows.stream()
                .filter(row -> query.text().equals(row.text()));
        if (query.typeId().isPresent()) {
            int typeId = query.typeId().getAsInt();
            // CoveredText query filters by type — we store role as string representation of typeId
            stream = stream.filter(row -> {
                try {
                    return Integer.parseInt(row.role()) == typeId;
                } catch (NumberFormatException e) {
                    return false;
                }
            });
        }
        return stream;
    }

    // ========== Internal Types ==========

    /**
     * SoFA type enum.
     */
    public enum SofaType {
        TEXT,
        BYTES,
        URI
    }

    /**
     * Internal record representing a Subject of Analysis (SoFA).
     */
    public record DUASofa(
            long fsRef,
            String sofaId,
            String localName,
            byte[] data,
            SofaType type,
            long createdEpochMs
    ) {
        public DUASofa {
            if (fsRef < 0) {
                throw new IllegalArgumentException("fsRef must not be negative");
            }
            Objects.requireNonNull(sofaId, "sofaId");
            Objects.requireNonNull(localName, "localName");
            Objects.requireNonNull(data, "data");
            Objects.requireNonNull(type, "type");
        }

        /**
         * Returns the sofa text decoded as UTF-8.
         *
         * @return the text content
         */
        public String text() {
            return new String(data, StandardCharsets.UTF_8);
        }

        /**
         * Creates a TEXT SoFA with the given text content.
         *
         * @param fsRef     the fsRef
         * @param sofaId    the sofa ID
         * @param localName the local name
         * @param text      the text content
         * @return a new TEXT SoFA
         */
        public static DUASofa ofText(long fsRef, String sofaId, String localName, String text) {
            Objects.requireNonNull(text, "text");
            return new DUASofa(fsRef, sofaId, localName, text.getBytes(StandardCharsets.UTF_8), SofaType.TEXT,
                    System.currentTimeMillis());
        }
    }
}
