package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.ErrorHandler;
import org.xml.sax.ContentHandler;

import org.apache.uima.util.XMLSerializer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;

public class SerDeUtils {

    /**
     * Reusable serialization buffer to avoid repeated large allocations per request.
     * One buffer is kept per thread to stay thread-safe.
     */
    public static final ThreadLocal<ByteArrayOutputStream> SERIALIZE_BUFFER =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(1024 * 1024));

    private SerDeUtils() {
    }

    public static final class XmiSharedIo {

        private static final ThreadLocal<XmiSerializationSharedData> SHARED =
            ThreadLocal.withInitial(XmiSerializationSharedData::new);

        private XmiSharedIo() {}

        public static void serialize(CAS cas,
                                     ContentHandler handler,
                                     ErrorHandler errorHandler) throws SAXException {
            XmiSerializationSharedData shared = SHARED.get();
            new XmiCasSerializer(cas.getTypeSystem())
                .serialize(cas, handler, errorHandler, shared, null);
        }

        public static void deserialize(InputStream in,
                                       CAS cas,
                                       boolean lenient) throws SAXException, IOException {
            XmiSerializationSharedData shared = SHARED.get();
            XmiCasDeserializer.deserialize(in, cas, lenient, shared);
        }
    }

    public static final class XmiLoggingErrorHandler implements ErrorHandler {
        private final DUUILogger logger;
        private final DUUIEvent.Context context;
        private final DUUIDocument document;

        public XmiLoggingErrorHandler(DUUILogger logger, DUUIEvent.Context context, DUUIDocument document) {
            this.logger = logger;
            this.context = context;
            this.document = document;
        }

        @Override
        public void warning(SAXParseException e) {
            logger.warn(
                context,
                "XMI serialization warning for %s: %s",
                document.getPath(),
                e.getMessage()
            );
        }

        @Override
        public void error(SAXParseException e) throws SAXException {
            document.setError(e.toString());
            logger.error(
                DUUIEvent.Context.readerError(
                    document.getPath(),
                    e
                ),
                "XMI serialization error for %s: %s",
                document.getPath(),
                e.getMessage()
            );
            throw e; // keep behavior: fail on error
        }

        @Override
        public void fatalError(SAXParseException e) throws SAXException {
            document.setError(e.toString());
            logger.error(
                DUUIEvent.Context.readerError(
                    document.getPath(),
                    e
                ),
                "XMI serialization FATAL error for %s: %s",
                document.getPath(),
                e.getMessage()
            );
            throw e;
        }
    }

    /**
     * Serialize the given CAS to XMI and optionally compress the result into the provided
     * {@link ByteArrayOutputStream}, returning the resulting byte array.
     * @throws SAXException 
     * @throws CompressorException 
     */
    public static byte[] serializeAndMaybeCompress(
        JCas jCas,
        String outputExtension,
        ErrorHandler handler,
        ByteArrayOutputStream outputStream
    ) throws IOException, SAXException, CompressorException {

        outputStream.reset();

        CompressorOutputStream compressorStream = null;
        XMLSerializer sax2xml;

        try {
            if (outputExtension != null) {
                if (outputExtension.equalsIgnoreCase(".gz")) {
                    compressorStream = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.GZIP, outputStream);
                } else if (outputExtension.equalsIgnoreCase(".xz")) {
                    compressorStream = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.XZ, outputStream);
                } else if (outputExtension.equalsIgnoreCase(".bz2")) {
                    compressorStream = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.BZIP2, outputStream);
                }
            }

            if (compressorStream != null) {
                sax2xml = new XMLSerializer(compressorStream);
            } else {
                sax2xml = new XMLSerializer(outputStream);
            }

            try {
                XmiSharedIo.serialize(jCas.getCas(), sax2xml.getContentHandler(), handler);
            } catch (SAXException e) {
                sax2xml.setOutputProperty(javax.xml.transform.OutputKeys.VERSION, "1.1");
                XmiSharedIo.serialize(jCas.getCas(), sax2xml.getContentHandler(), handler);
            }
        } finally {
            if (compressorStream != null) {
                try {
                    compressorStream.close();
                } catch (IOException ignore) {
                }
            }
        }

        return outputStream.toByteArray();
    }
}
