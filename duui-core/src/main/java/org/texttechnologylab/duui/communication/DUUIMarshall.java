package org.texttechnologylab.duui.communication;

import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public record DUUIMarshall(
    DUUICommunicationLayer communicationLayer,
    Map<String, String> parameters,
    String sourceView,
    String targetView
) {
    public void marshall(JCas jcas, OutputStream output) throws CASException {
        communicationLayer.serialize(jcas, output, parameters, sourceView);
    }

    public void unmarshall(JCas jcas, InputStream input) throws CASException {
        communicationLayer.deserialize(jcas, input, targetView);
    }
}
