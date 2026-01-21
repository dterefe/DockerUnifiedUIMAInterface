package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.util.List;

/**
 * Composite handler for binary/bytes documents with configurable MIME type.
 * Wraps an underlying handler and ensures all documents have the specified MIME type set.
 * Uses the default standardDeserialize for binary sofa handling.
 */
public final class BytesDocumentHandler implements IDUUIDocumentHandler {
    private final IDUUIDocumentHandler delegate;
    private final String mimeType;

    /**
     * @param delegate the underlying handler to wrap
     * @param mimeType the MIME type to assign to all documents (e.g., "application/pdf")
     */
    public BytesDocumentHandler(IDUUIDocumentHandler delegate, String mimeType) {
        this.delegate = delegate;
        this.mimeType = mimeType == null ? "" : mimeType;
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
        delegate.writeDocument(document, path);
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        DUUIDocument doc = delegate.readDocument(path);
        doc.setMimeType(mimeType);
        return doc;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String ext, boolean recursive) throws IOException {
        List<DUUIDocument> docs = delegate.listDocuments(path, ext, recursive);
        for (DUUIDocument d : docs) {
            d.setMimeType(mimeType);
        }
        return docs;
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
