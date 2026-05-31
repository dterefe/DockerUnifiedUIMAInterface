package org.texttechnologylab.duui.dua;

import java.util.Objects;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.dua.projection.DUAProjection;
import org.texttechnologylab.duui.dua.projection.DUAProjectionType;

public final class JDUA<T> {
    private final JCas view;
    private final DUABackend backend;
    private final DUAProjection<T> projection;

    JDUA(JCas view, DUABackend backend, DUAProjection<T> projection) {
        this.view = Objects.requireNonNull(view, "view");
        this.backend = Objects.requireNonNull(backend, "backend");
        this.projection = Objects.requireNonNull(projection, "projection");
    }

    public JCas view() {
        return view;
    }

    public CAS cas() {
        return view.getCas();
    }

    public DUABackend backend() {
        return backend;
    }

    public DUAProjection<T> projection() {
        return projection;
    }

    public DUAProjectionType<T> projectionType() {
        return projection.type();
    }

    public String typeName() {
        return projection.typeName();
    }

    public Class<T> markerClass() {
        return projection.markerClass();
    }

    public String modeName() {
        return projection.modeName();
    }
}
