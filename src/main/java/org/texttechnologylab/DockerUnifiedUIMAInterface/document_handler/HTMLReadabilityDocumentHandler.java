package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIDocumentDecoder;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability.HTMLReadabilityLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SerDeUtils;

/**
 * Composite handler for HTML documents using the readability library to extract content.
 * Wraps an underlying handler (typically DUUILocalDocumentHandler) and intercepts
 * deserialization to apply HTML-specific logic.
 */
public final class HTMLReadabilityDocumentHandler implements IDUUIDocumentHandler {
    private final IDUUIDocumentHandler delegate;

    public HTMLReadabilityDocumentHandler(IDUUIDocumentHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
        delegate.writeDocument(document, path);
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        DUUIDocument doc = delegate.readDocument(path);
        SerDeUtils.ensureHtmlMimeForRead(doc, path, this.getClass().getSimpleName());
        return doc;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String ext, boolean recursive) throws IOException {
        String normalizedExt = SerDeUtils.normalizeAndRequireHtmlExtension(ext, this.getClass().getSimpleName());

        List<DUUIDocument> docs = delegate.listDocuments(path, normalizedExt, recursive);
        for (DUUIDocument d : docs) {
            d.setMimeType(SerDeUtils.MIME_TEXT_HTML);
        }
        return docs;
    }

    /**
     * Custom deserialization: use HTMLReadabilityLoader to extract readable content from HTML.
     * Falls back to standard text deserialization if readability loading fails.
     */
    @Override
    public void deserialize(DUUIDocument document, JCas cas, DUUIDocumentReader.DeserializationContext ctx) throws Exception {
        SerDeUtils.requireHtmlMime(document, this.getClass().getSimpleName());

        try (InputStream decoded = DUUIDocumentDecoder.decode(document)) {
            HTMLReadabilityLoader.load(decoded, cas);
        }
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
