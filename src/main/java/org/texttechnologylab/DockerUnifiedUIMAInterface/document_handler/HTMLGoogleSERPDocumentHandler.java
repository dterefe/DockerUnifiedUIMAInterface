package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIDocumentDecoder;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.google.HTMLGoogleSERPLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SerDeUtils;

/**
 * Composite handler for Google SERP (Search Engine Results Page) HTML documents.
 * Wraps an underlying handler and applies Google SERP-specific extraction logic during deserialization.
 */
public final class HTMLGoogleSERPDocumentHandler implements IDUUIDocumentHandler {
    private final IDUUIDocumentHandler delegate;

    public HTMLGoogleSERPDocumentHandler(IDUUIDocumentHandler delegate) {
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
     * Custom deserialization: use HTMLGoogleSERPLoader to extract search results and metadata.
     */
    @Override
    public void deserialize(DUUIDocument document, JCas cas, DUUIDocumentReader.DeserializationContext ctx) throws Exception {
        SerDeUtils.requireHtmlMime(document, this.getClass().getSimpleName());

        try (InputStream decoded = DUUIDocumentDecoder.decode(document)) {
            HTMLGoogleSERPLoader.load(decoded, cas);
        }
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
