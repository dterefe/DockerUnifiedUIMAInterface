package org.texttechnologylab.duui.dua.uima;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.Backend;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.jcas.JCas;

public final class DUACasBackendInstaller {
    private DUACasBackendInstaller() {
    }

    public static void install(JCas view, Backend backend) {
        install(view.getCas(), backend);
    }

    public static void install(CAS cas, Backend backend) {
        if (!(cas instanceof CASImpl casImpl)) {
            throw new IllegalArgumentException("DUA backend requires the shadowed UIMA CASImpl on the classpath.");
        }
        casImpl.getBaseCAS().backend(backend);
    }
}
