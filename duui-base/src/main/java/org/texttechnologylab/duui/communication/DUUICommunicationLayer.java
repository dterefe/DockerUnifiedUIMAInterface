package org.texttechnologylab.duui.communication;

import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface DUUICommunicationLayer {
    void serialize(JCas sourceCas, OutputStream output, Map<String, String> parameters, String sourceView)
        throws CASException;

    void deserialize(JCas targetCas, InputStream input, String targetView) throws CASException;

    default boolean supportsProcess() {
        return false;
    }

    default void process(JCas sourceCas, Object requestHandler, Map<String, String> parameters, JCas targetCas) throws Exception {
        throw new UnsupportedOperationException("Communication layer does not support process().");
    }

    DUUICommunicationLayer copy() throws Exception;
}
