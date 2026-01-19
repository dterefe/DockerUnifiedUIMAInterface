package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.bson.Document;
import org.bson.types.ObjectId;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSDownloadStream;

import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;
import org.texttechnologylab.annotation.AnnotationComment;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

/**
 * Handler for reading XMI documents from MongoDB GridFS (GerParCor corpus style).
 * Read-only handler that retrieves documents by MongoDB ObjectId and optionally enriches metadata.
 */
public final class GerParCorDocumentHandler implements IDUUIDocumentHandler {
    private final MongoDBConnectionHandler mongo;
    private final GridFSBucket gridFS;
    private final String queryJson;
    private final boolean overrideMeta;

    /**
     * @param mongo MongoDB connection handler configured with the target database
     * @param gridFS GridFS bucket for downloading document payloads
     * @param queryJson BSON query string for filtering documents (e.g., "{}")
     * @param overrideMeta whether to override DocumentMetaData with MongoDB ObjectId
     */
    public GerParCorDocumentHandler(MongoDBConnectionHandler mongo, GridFSBucket gridFS, String queryJson, boolean overrideMeta) {
        this.mongo = mongo;
        this.gridFS = gridFS;
        this.queryJson = queryJson == null ? "{}" : queryJson;
        this.overrideMeta = overrideMeta;
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) {
        throw new UnsupportedOperationException("GerParCorDocumentHandler is read-only");
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) {
        List<DUUIDocument> docs = new ArrayList<>();
        Document query = Document.parse(queryJson);
        try (MongoCursor<Document> cursor = mongo.getDatabase().getCollection("fs.files").find(query).iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                ObjectId fileId = doc.getObjectId("_id");
                if (fileId == null) continue;
                DUUIDocument d = new DUUIDocument(fileId.toString(), "mongo://gerparcor/" + fileId);
                d.setMimeType("application/xmi+xml");
                docs.add(d);
            }
        }
        return docs;
    }

    @Override
    public DUUIDocument readDocument(String path) {
        String id = path.substring(path.lastIndexOf('/') + 1);
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("GerParCorDocumentHandler: invalid path (missing ObjectId): " + path);
        }
        DUUIDocument d = new DUUIDocument(id, path);
        d.setMimeType("application/xmi+xml");
        return d;
    }

    @Override
    public void deserialize(DUUIDocument document, JCas cas, DUUIDocumentReader.DeserializationContext ctx) throws Exception {
        String idStr = document.getName();
        ObjectId fileId = new ObjectId(idStr);

        // Download from GridFS
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GridFSDownloadStream stream = gridFS.openDownloadStream(fileId)) {
            byte[] buf = new byte[4096];
            int len;
            while ((len = stream.read(buf)) > 0) {
                baos.write(buf, 0, len);
            }
        }

        byte[] data = baos.toByteArray();

        // Create document and let standard deserialization handle decompression via decoder
        DUUIDocument xmiDoc = new DUUIDocument(document.getName(), document.getPath(), data);
        xmiDoc.setMimeType("application/xmi+xml");
        ctx.reader().standardDeserialize(xmiDoc, cas, ctx);

        // AnnotationComment("mongoid") — store the MongoDB ObjectId as annotation
        if (overrideMeta) {
            // Find or create AnnotationComment for the document annotation
            TOP docAnnotation = cas.getDocumentAnnotationFs();
            if (docAnnotation != null) {
                AnnotationComment id = new AnnotationComment(cas);
                id.setKey("mongoid");
                id.setValue(fileId.toString());
                id.setReference(docAnnotation);
                id.addToIndexes();
            }
        }
    }

    @Override
    public void shutdown() {
        // no resources to clean up
    }
}
