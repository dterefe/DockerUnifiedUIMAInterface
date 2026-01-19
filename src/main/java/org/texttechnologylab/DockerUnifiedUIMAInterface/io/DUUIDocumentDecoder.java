package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SerDeUtils;

import java.io.IOException;
import java.io.InputStream;

public class DUUIDocumentDecoder {
    private DUUIDocumentDecoder() {}

    public static InputStream decode(DUUIDocument document) throws IOException {
        // Prefer mime type if set; otherwise fall back to file extension
        String mimeType = document.getMimeType() == null ? "" : document.getMimeType();
        String fileExtension = document.getFileExtension().replace(".", "").toLowerCase();

        String compression = "";

        // Check each compression format by mime type or extension
        if (SerDeUtils.mimeMatches("application/gzip", mimeType) || "gzip".equalsIgnoreCase(fileExtension) || "gz".equalsIgnoreCase(fileExtension)) {
            compression = CompressorStreamFactory.GZIP;
        } else if (SerDeUtils.mimeMatches("application/x-xz", mimeType) || "xz".equalsIgnoreCase(fileExtension)) {
            compression = CompressorStreamFactory.XZ;
        } else if (SerDeUtils.mimeMatches("application/x-bzip2", mimeType) || "bz2".equalsIgnoreCase(fileExtension)) {
            compression = CompressorStreamFactory.BZIP2;
        }

        if (!compression.isEmpty()) {
            try {
                return new CompressorStreamFactory()
                    .createCompressorInputStream(
                        compression,
                        document.toInputStream()
                    );
            } catch (CompressorException e) {
                throw new IOException("Document is not in the correct format: " + compression, e);
            }
        }

        return document.toInputStream();
    }
}

