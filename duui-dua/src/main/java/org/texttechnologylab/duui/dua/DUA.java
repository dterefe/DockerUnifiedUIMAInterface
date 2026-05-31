package org.texttechnologylab.duui.dua;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CAS;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.dua.projection.DUAProjection;
import org.texttechnologylab.duui.dua.projection.DUAProjectionType;
import org.texttechnologylab.duui.dua.projection.DUAProjectionTypes;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;
import org.texttechnologylab.duui.dua.query.DUATextQuery;
import org.texttechnologylab.duui.dua.query.DUATypeQuery;
import org.texttechnologylab.duui.dua.query.DUAValueQuery;
import org.texttechnologylab.duui.dua.store.DUAProjectionStore;
import org.texttechnologylab.duui.dua.store.DUATextRow;
import org.texttechnologylab.duui.dua.store.DUATypeNode;
import org.texttechnologylab.duui.dua.store.DUAValueRow;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

/**
 * Minimal public DUA universe handle.
 *
 * <p>This is intentionally small during the reset: it owns a public
 * {@link DUABackend} facade and, when present, the UIMA view installed
 * on that backend. Archive constants remain only for the reset persistence
 * payload layout.</p>
 */
public final class DUA implements AutoCloseable {
    public static final String FORMAT = "DUA";
    public static final int FORMAT_VERSION = 1;
    public static final String MANIFEST = "dua.json";
    public static final String ARTIFACTS = "artifacts/";
    public static final String TYPESYSTEMS = "typesystems/";
    public static final String CAS = "cas/";
    public static final String INDEXES = "indexes/";
    public static final String STORES = "stores/";

    private final DUABackend backend;
    private final JCas view;

    private DUA(DUABackend backend, JCas view) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.view = view;
    }

    public static DUA open(DUABackend backend) {
        return new DUA(backend, null);
    }

    public static DUA create() throws UIMAException {
        return create(DUABackend.inMemory());
    }

    public static DUA create(DUABackend backend) throws UIMAException {
        JCas view = JCasFactory.createJCas();
        backend.installInto(view);
        return new DUA(backend, view);
    }

    public static DUA attach(JCas view, DUABackend backend) {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(backend, "backend");
        backend.installInto(view);
        return new DUA(backend, view);
    }

    public DUABackend backend() {
        return backend;
    }

    public Optional<JCas> view() {
        return Optional.ofNullable(view);
    }

    public Optional<CAS> cas() {
        return view().map(JCas::getCas);
    }

    public DUAProjectionStore projections() {
        return backend.projections();
    }

    public <T> DUAProjection<T> registerProjection(DUAProjectionType<T> type) {
        return backend.registerProjection(type);
    }

    public <T> JDUA<T> project(DUAProjectionType<T> type) {
        return backend.project(requireView(), type);
    }

    public JDUA<DUAProjectionTypes.Corpus> projectCorpus() {
        return project(DUAProjectionTypes.CORPUS);
    }

    public JDUA<DUAProjectionTypes.Document> projectDocument() {
        return project(DUAProjectionTypes.DOCUMENT);
    }

    public JCas requireView() {
        return view().orElseThrow(() -> new IllegalStateException("This DUA handle has no attached UIMA view"));
    }

    public CAS requireCas() {
        return requireView().getCas();
    }

    public DUAWriteResult indexAnnotation(DUAAnnotationSpan span) {
        return backend.indexAnnotation(span);
    }

    public DUAWriteResult indexType(DUATypeNode node) {
        return backend.indexType(node);
    }

    public Stream<DUAAnnotationSpan> findAnnotations(DUAAnnotationSpanQuery query) {
        return backend.findAnnotations(query);
    }

    public Stream<DUATypeNode> findTypes(DUATypeQuery query) {
        return backend.typesystemIndex().find(query);
    }

    public Stream<DUAValueRow> findValues(DUAValueQuery query) {
        return backend.values().find(query);
    }

    public Stream<DUATextRow> findTexts(DUATextQuery query) {
        return backend.texts().find(query);
    }

    @Override
    public void close() {
        backend.close();
    }
}
