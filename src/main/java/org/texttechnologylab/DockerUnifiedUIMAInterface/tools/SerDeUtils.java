package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.StringUtils;
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
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContexts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;

public class SerDeUtils {

    /**
     * Reusable serialization buffer to avoid repeated large allocations per request.
     * One buffer is kept per thread to stay thread-safe.
     */
    public static final ThreadLocal<ByteArrayOutputStream> SERIALIZE_BUFFER =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(1024 * 1024));

    private SerDeUtils() {
    }

    public static final String MIME_TEXT_HTML = Mime.MIME_TEXT_HTML;

    public enum MimePrimitive {
        TEXT("text"),
        APPLICATION("application"),
        AUDIO("audio"),
        VIDEO("video"),
        IMAGE("image"),
        MULTIPART("multipart"),
        MESSAGE("message"),
        MODEL("model"),
        FONT("font"),
        CHEMICAL("chemical"),
        UNKNOWN("");

        private final String major;

        MimePrimitive(String major) {
            this.major = major;
        }

        public String major() {
            return major;
        }

        private static MimePrimitive fromMajorOrUnknown(String major) {
            if (major == null || major.isBlank()) return UNKNOWN;
            String normalized = major.trim().toLowerCase(Locale.ROOT);
            for (MimePrimitive p : values()) {
                if (!p.major.isEmpty() && p.major.equals(normalized)) {
                    return p;
                }
            }
            return UNKNOWN;
        }
    }

    public static byte[] getBytes(JCas cas) {
        if (cas.getDocumentText() != null) {
            return cas.getDocumentText().getBytes(StandardCharsets.UTF_8);
        } else {
            try {
                return cas.getSofaDataStream().readAllBytes();
            }
            catch (Exception e) {
                return new byte[0];
            }
        }
    }

    /**
     * Ensure the document carries a canonical MIME type.
     */
    public static void ensureCanonicalMimeType(DUUIDocument document) {
        Mime.ensureCanonicalMimeType(document);
    }

    /** Returns true if the given name/path looks like an XMI document (supports compressed variants). */
    public static boolean isXmiPath(String nameOrPath) {
        return Mime.isXmiPath(nameOrPath);
    }

    /** Returns true if the given extension is a supported compression extension (.gz/.xz/.bz2). */
    public static boolean isCompressedExtension(String ext) {
        return Mime.isCompressedExtension(ext);
    }

    /**
     * Validate that {@code ext} represents HTML and return the normalized extension.
     */
    public static String normalizeAndRequireHtmlExtension(String ext, String handlerName) {
        return Mime.normalizeAndRequireHtmlExtension(ext, handlerName);
    }

    /**
     * For read-paths: if the file path indicates HTML or the MIME is missing, enforce text/html;
     * otherwise validate that the MIME is compatible with text/html.
     */
    public static void ensureHtmlMimeForRead(DUUIDocument doc, String path, String handlerName) {
        Mime.ensureHtmlMimeForRead(doc, path, handlerName);
    }

    public static void requireHtmlMime(DUUIDocument document, String handlerName) {
        Mime.requireHtmlMime(document, handlerName);
    }

    /**
     * Match an actual MIME against a primitive (top-level type) such as {@code text} or {@code audio}.
     * Ignores MIME parameters (everything after {@code ;}) and case.
     */
    public static boolean mimeMatches(MimePrimitive expectedPrimitive, String actual) {
        if (expectedPrimitive == null || expectedPrimitive == MimePrimitive.UNKNOWN) return false;
        return expectedPrimitive == Mime.primitiveOf(actual);
    }

    /**
     * Match an actual MIME against an expected value.
     *
     * Supported expected formats:
     * - exact type/subtype (e.g. {@code text/html})
     * - major wildcard (e.g. {@code text/*})
     *
     * Ignores MIME parameters (everything after {@code ;}) and case.
     */
    public static boolean mimeMatches(String expected, String actual) {
        if (expected == null || expected.isBlank() || actual == null || actual.isBlank()) return false;

        String expectedNormalized = Mime.normalizeMimeTypeFast(expected);
        if (expectedNormalized.isBlank()) return false;

        // major/* wildcard
        if (expectedNormalized.endsWith("/*")) {
            int slash = expectedNormalized.indexOf('/');
            if (slash <= 0) return false;
            String major = expectedNormalized.substring(0, slash);
            MimePrimitive expectedPrimitive = MimePrimitive.fromMajorOrUnknown(major);
            return mimeMatches(expectedPrimitive, actual);
        }

        // exact type/subtype
        return Mime.normalizeMimeTypeFast(actual).equals(expectedNormalized);
    }

    /**
     * Check if a MIME type represents XMI format (UIMA CAS serialization).
     * Supports: application/xmi, application/x-xmi, application/vnd.uima.cas+xmi
     */
    public static boolean isMimeXmi(String mimeType) {
        return Mime.isMimeXmi(mimeType);
    }

    /**
     * Check if a MIME type represents a compressed format.
     * Supports: application/gzip, application/x-xz, application/x-bzip2
     */
    public static boolean isMimeCompressed(String mimeType) {
        return Mime.isMimeCompressed(mimeType);
    }

    /**
     * Infer MIME type from document path/name by file extension, with safe fallback.
     * Returns empty string if no recognized extension found.
     */
    public static String inferMimeTypeFromPathOrDefault(DUUIDocument document) {
        return Mime.inferMimeType(document);
    }

    /**
     * Infer MIME from a declared MIME and a filename/path (no DUUIDocument required).
     * Preference: declared MIME (if informative), then filename-based guess.
     * Returns normalized MIME or empty string.
     */
    public static String inferMimeTypeFromPathOrDefault(String declaredMime, String fileName) {
        return Mime.inferMimeType(declaredMime, fileName);
    }

    /**
     * Infer MIME from a declared MIME, a filename/path and optional bytes.
     * Preference: declared MIME, then filename-based guess, then byte-sniffing.
     */
    public static String inferMimeTypeFromPathOrDefault(String declaredMime, String fileName, byte[] bytes) {
        return Mime.inferMimeType(declaredMime, fileName, bytes);
    }

    /**
     * Attempt to repair invalid XML by removing characters that are not allowed in XML.
     * Returns an Optional containing a new InputStream with the repaired XML if repair was successful,
     * or an empty Optional if the exception was not an XML parse error or repair failed.
     *
     * @param cause The exception to check (typically a SAXParseException or similar)
     * @param xml   The XML string to repair
     * @return An Optional containing a repaired InputStream, or empty if repair was not applicable
     */
    public static Optional<InputStream> tryRepairXmi(Throwable cause, String xml) {
        return Xml.tryRepairXmi(cause, xml);
    }

    /**
     * Attempt to repair invalid XML by removing characters that are not allowed in XML.
     * Returns an Optional containing a new InputStream with the repaired XML if repair was successful.
     *
     * @param cause The exception to check
     * @param in    The InputStream to repair (will be read into memory)
     * @return An Optional containing a repaired InputStream, or empty if repair was not applicable
     */
    public static Optional<InputStream> tryRepairXmi(Throwable cause, InputStream in) throws IOException {
        return Xml.tryRepairXmi(cause, in);
    }

    /** Internal helpers grouped by concern. Public API delegates here. */
    public static final class Mime {
        static final String MIME_TEXT_HTML = "text/html";

        static final String MIME_APPLICATION_XMI = "application/xmi";
        static final String MIME_APPLICATION_XML = "application/xml";
        static final String MIME_TEXT_PLAIN = "text/plain";

        private static final String EXT_GZ = ".gz";
        private static final String EXT_XZ = ".xz";
        private static final String EXT_BZ2 = ".bz2";
        private static final String EXT_TXT = ".txt";

        private static final String[] XMI_SUFFIXES = { ".xmi", ".xmi" + EXT_GZ, ".xmi" + EXT_XZ, ".xmi" + EXT_BZ2 };
        private static final String[] XML_SUFFIXES = { ".xml", ".xml" + EXT_GZ, ".xml" + EXT_XZ, ".xml" + EXT_BZ2 };
        private static final String[] HTML_EXTENSIONS = { ".html", ".htm", ".html" + EXT_GZ, ".htm" + EXT_GZ };

        private static final String[] XMI_MIME_TYPES = {
            MIME_APPLICATION_XMI,
            "application/x-xmi",
            "application/vnd.uima.cas+xmi",
        };

        private static final String[] COMPRESSED_MIME_TYPES = {
            "application/gzip",
            "application/x-xz",
            "application/x-bzip2",
        };

        private static final String[] UNHELPFUL_MIME_TYPES = {
            "application/octet-stream",
            "binary/octet-stream",
            "application/unknown",
            "content/unknown",
            "unknown/unknown",
            "application/x-unknown",
        };

        private Mime() {}

        static MimePrimitive primitiveOf(String mimeType) {
            String normalized = normalizeMimeTypeFast(mimeType);
            if (normalized.isBlank()) return MimePrimitive.UNKNOWN;
            int slash = normalized.indexOf('/');
            String major = slash > 0 ? normalized.substring(0, slash) : normalized;
            return MimePrimitive.fromMajorOrUnknown(major);
        }

        private static boolean isMissingOrUnhelpfulMime(String normalizedMime) {
            if (normalizedMime == null || normalizedMime.isBlank()) return true;
            for (String bad : UNHELPFUL_MIME_TYPES) {
                if (normalizedMime.equals(bad)) return true;
            }
            return false;
        }

        public static String inferMimeType(DUUIDocument document) {
            return inferMimeType(document.getMimeType(), document.getName(), document.getBytes());
        }

        /**
         * Infer MIME from a declared MIME and a filename/path (no DUUIDocument required).
         * Preference: declared MIME (if informative), then filename-based guess.
         * Does not perform byte-based sniffing.
         */
        public static String inferMimeType(String declaredMime, String nameOrPath) {
            String normalizedDeclared = normalizeMimeTypeFast(declaredMime);
            if (!isMissingOrUnhelpfulMime(normalizedDeclared)) return normalizedDeclared;
            String byName = guessMimeTypeFromNameOrEmpty(nameOrPath);
            return byName == null ? "" : byName;
        }

        /**
         * Infer MIME from declared MIME, filename/path and optional bytes (sniffed).
         * Preference: declared MIME (if informative), filename-based guess, then
         * stream-based sniffing when bytes are provided.
         */
        public static String inferMimeType(String declaredMime, String nameOrPath, byte[] bytes) {
            String normalizedDeclared = normalizeMimeTypeFast(declaredMime);
            if (!isMissingOrUnhelpfulMime(normalizedDeclared)) return normalizedDeclared;
            String byName = inferMimeType(declaredMime, nameOrPath);
            if (!StringUtils.isNotBlank(byName)) return byName;
            if (bytes != null && bytes.length > 0) {
                try {
                    String sniffed = guessMimeTypeFromStream(new ByteArrayInputStream(bytes));
                    if (!sniffed.isBlank()) return sniffed;
                } catch (IOException ignored) {
                }
            }
            return "";
        }

        private static String normalizeMimeTypeFast(String mimeType) {
            if (mimeType == null) return "";

            int end = mimeType.indexOf(';');
            if (end < 0) end = mimeType.length();

            int start = 0;
            while (start < end && Character.isWhitespace(mimeType.charAt(start))) start++;
            while (end > start && Character.isWhitespace(mimeType.charAt(end - 1))) end--;

            if (end <= start) return "";
            return mimeType.substring(start, end).toLowerCase(Locale.ROOT);
        }

        private static boolean equalsAnyNormalized(String mimeType, String[] allowedNormalized) {
            String normalized = normalizeMimeTypeFast(mimeType);
            if (normalized.isBlank()) return false;
            for (String allowed : allowedNormalized) {
                if (normalized.equals(allowed)) return true;
            }
            return false;
        }

        private static boolean endsWithAny(String value, String[] suffixes) {
            if (value == null || value.isBlank()) return false;
            for (String suffix : suffixes) {
                if (value.endsWith(suffix)) return true;
            }
            return false;
        }

        private static String normalizePathLower(String nameOrPath) {
            if (nameOrPath == null) return "";
            String trimmed = nameOrPath.trim();
            if (trimmed.isEmpty()) return "";
            return trimmed.toLowerCase(Locale.ROOT);
        }

        static boolean isXmiPath(String nameOrPath) {
            return endsWithAny(normalizePathLower(nameOrPath), XMI_SUFFIXES);
        }

        static boolean isXmlPath(String nameOrPath) {
            return endsWithAny(normalizePathLower(nameOrPath), XML_SUFFIXES);
        }

        static boolean isTxtPath(String nameOrPath) {
            return normalizePathLower(nameOrPath).endsWith(EXT_TXT);
        }

        static boolean isGzipPath(String nameOrPath) {
            return normalizePathLower(nameOrPath).endsWith(EXT_GZ);
        }

        static boolean isXzPath(String nameOrPath) {
            return normalizePathLower(nameOrPath).endsWith(EXT_XZ);
        }

        static boolean isBzip2Path(String nameOrPath) {
            return normalizePathLower(nameOrPath).endsWith(EXT_BZ2);
        }

        static boolean isCompressedPath(String nameOrPath) {
            String lower = normalizePathLower(nameOrPath);
            return lower.endsWith(EXT_GZ) || lower.endsWith(EXT_XZ) || lower.endsWith(EXT_BZ2);
        }

        static boolean isCompressedExtension(String ext) {
            String normalized = normalizeExtension(ext);
            return EXT_GZ.equals(normalized) || EXT_XZ.equals(normalized) || EXT_BZ2.equals(normalized);
        }

        static void ensureCanonicalMimeType(DUUIDocument document) {
            if (document == null) return;

            // Do not rewrite caller-provided MIME strings.
            // We only parse them (strip parameters + lowercase) for comparison.
            String normalizedExisting = normalizeMimeTypeFast(document.getMimeType());

            // If it's already informative, stop here.
            if (!isMissingOrUnhelpfulMime(normalizedExisting)) {
                return;
            }

            // Infer from best available metadata (name → path → fileExtension).
            String inferred = inferMimeType(document);
            if (!inferred.isBlank()) {
                document.setMimeType(inferred);
                return;
            }

            // Last resort: sniff from bytes (if available) using JDK heuristics.
            // Keep best-effort and swallow failures.
            try {
                byte[] bytes = document.getBytes();
                if (bytes != null && bytes.length > 0) {
                    String sniffed = guessMimeTypeFromStream(new ByteArrayInputStream(bytes));
                    if (!sniffed.isBlank()) {
                        document.setMimeType(sniffed);
                    }
                }
            } catch (Exception ignored) {
                // best-effort MIME inference should never fail the request
            }
        }

        static String normalizeMimeType(String mimeType) {
            return normalizeMimeTypeFast(mimeType);
        }

        static String guessMimeTypeFromExtensionOrEmpty(String nameOrPath) {
            if (nameOrPath == null || nameOrPath.isBlank()) return "";
            String lower = nameOrPath.toLowerCase(Locale.ROOT);

            // XMI (including compressed variants)
            if (endsWithAny(lower, XMI_SUFFIXES)) {
                return MIME_APPLICATION_XMI;
            }

            // XML (including compressed variants)
            if (endsWithAny(lower, XML_SUFFIXES)) {
                return MIME_APPLICATION_XML;
            }

            if (lower.endsWith(EXT_TXT)) {
                return MIME_TEXT_PLAIN;
            }

            if (isHtmlPath(lower)) {
                return MIME_TEXT_HTML;
            }

            return "";
        }

        public static String guessMimeTypeFromNameOrEmpty(String nameOrPath) {
            String byExt = guessMimeTypeFromExtensionOrEmpty(nameOrPath);
            if (!byExt.isBlank()) return byExt;

            if (nameOrPath == null || nameOrPath.isBlank()) return "";
            return normalizeMimeType(URLConnection.guessContentTypeFromName(nameOrPath));
        }

        static String guessMimeTypeFromStream(InputStream in) throws IOException {
            if (in == null) return "";

            InputStream buffered = in.markSupported() ? in : new BufferedInputStream(in);
            buffered.mark(64 * 1024);
            String guessed = URLConnection.guessContentTypeFromStream(buffered);
            try {
                buffered.reset();
            } catch (IOException ignore) {
                // best-effort reset; caller must not rely on stream position
            }
            return normalizeMimeType(guessed);
        }

        static String normalizeExtension(String ext) {
            String normalized = ext == null ? "" : ext.trim().toLowerCase(Locale.ROOT);
            if (normalized.isBlank()) return "";
            return normalized.startsWith(".") ? normalized : "." + normalized;
        }

        static boolean isHtmlExtension(String ext) {
            String normalized = normalizeExtension(ext);
            return endsWithAny(normalized, HTML_EXTENSIONS);
        }

        static boolean isHtmlPath(String path) {
            String fileName = "";
            if (path != null) {
                try {
                    fileName = Paths.get(path).getFileName().toString();
                } catch (RuntimeException ignored) {
                    fileName = path;
                }
            }

            String lower = fileName.toLowerCase(Locale.ROOT);

            String ext;
            if (lower.endsWith(EXT_GZ)) {
                // preserve multi-suffix extension such as .html.gz
                int beforeGz = lower.length() - 4;
                int prevDot = lower.lastIndexOf('.', beforeGz - 1);
                ext = prevDot >= 0 ? lower.substring(prevDot) : EXT_GZ;
            } else {
                int lastDot = lower.lastIndexOf('.');
                ext = lastDot >= 0 ? lower.substring(lastDot) : "";
            }

            return isHtmlExtension(ext);
        }

        static void requireHtmlExtension(String ext, String handlerName) {
            if (!isHtmlExtension(ext)) {
                throw new IllegalArgumentException(
                    handlerName + " only supports .html/.htm (and .html.gz/.htm.gz) (got: " + ext + ")"
                );
            }
        }

        static String normalizeAndRequireHtmlExtension(String ext, String handlerName) {
            requireHtmlExtension(ext, handlerName);
            return normalizeExtension(ext);
        }

        static void ensureHtmlMimeForRead(DUUIDocument doc, String path, String handlerName) {
            String mime = doc.getMimeType();
            if (isHtmlExtension(doc.getFileExtension())) {
                doc.setMimeType(MIME_TEXT_HTML);
                return;
            }
            if (!SerDeUtils.mimeMatches(MIME_TEXT_HTML, mime)) {
                throw new IllegalArgumentException(
                    handlerName + " only supports " + MIME_TEXT_HTML + " (got: " + mime + ") for " + path
                );
            }
        }

        static void requireHtmlMime(DUUIDocument document, String handlerName) {
            if (!SerDeUtils.mimeMatches(MIME_TEXT_HTML, document.getMimeType())) {
                throw new IllegalArgumentException(
                    handlerName + " only supports " + MIME_TEXT_HTML + " (got: " + document.getMimeType() + ") for " + document.getPath()
                );
            }
        }

        static boolean isMimeXmi(String mimeType) {
            return equalsAnyNormalized(mimeType, XMI_MIME_TYPES);
        }

        static boolean isMimeCompressed(String mimeType) {
            return equalsAnyNormalized(mimeType, COMPRESSED_MIME_TYPES);
        }
    }

    static final class Xml {
        private Xml() {}

        static Optional<InputStream> tryRepairXmi(Throwable cause, String xml) {
            if (!isXmlParseFailure(cause)) return Optional.empty();
            if (xml == null) return Optional.empty();
            try {
                String sanitized = sanitizeXml11(xml);
                return Optional.of(new ByteArrayInputStream(sanitized.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        static Optional<InputStream> tryRepairXmi(Throwable cause, InputStream in) throws IOException {
            if (!isXmlParseFailure(cause)) return Optional.empty();
            try {
                String xml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                return tryRepairXmi(cause, xml);
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        private static boolean isXmlParseFailure(Throwable t) {
            for (Throwable cur = t; cur != null; cur = cur.getCause()) {
                if (cur instanceof SAXParseException) return true;
                if (cur.getClass().getName().contains("SAXParseException")) return true;
            }
            return false;
        }

        /**
         * XML 1.1 invalid character handling (fast path).
         * This replaces the previous complex regex and follows the same intent:
         * drop disallowed control chars and Unicode non-characters.
         */
        private static boolean isInvalidXml11CodePoint(int cp) {
            // Disallowed C0 controls (XML 1.1)
            if ((cp >= 0x1 && cp <= 0x8)
                || (cp >= 0xB && cp <= 0xC)
                || (cp >= 0xE && cp <= 0x1F)
                || (cp >= 0x7F && cp <= 0x84)
                || (cp >= 0x86 && cp <= 0x9F)) {
                return true;
            }

            // Unicode non-characters
            if (cp >= 0xFDD0 && cp <= 0xFDDF) {
                return true;
            }
            int lower16 = cp & 0xFFFF;
            return lower16 == 0xFFFE || lower16 == 0xFFFF;
        }

        private static String sanitizeXml11(String xml) {
            if (xml == null || xml.isEmpty()) return "";
            StringBuilder out = new StringBuilder(xml.length());
            int i = 0;
            while (i < xml.length()) {
                int cp = xml.codePointAt(i);
                if (!isInvalidXml11CodePoint(cp)) {
                    out.appendCodePoint(cp);
                }
                i += Character.charCount(cp);
            }
            return out.toString();
        }
    }

    public static final class XmiSharedIo {

        private static final ThreadLocal<XmiSerializationSharedData> SHARED =
            ThreadLocal.withInitial(XmiSerializationSharedData::new);

        private XmiSharedIo() {}

        public static void serialize(CAS cas,
                                     ContentHandler handler,
                                     ErrorHandler errorHandler) throws SAXException {
            try {
                XmiSerializationSharedData shared = SHARED.get();
                new XmiCasSerializer(cas.getTypeSystem())
                    .serialize(cas, handler, errorHandler, shared, null);
            } finally {
                // Avoid retaining ThreadLocal state on short-lived virtual threads.
                if (Thread.currentThread().isVirtual()) {
                    SHARED.remove();
                }
            }
        }

        public static void deserialize(InputStream in,
                                       CAS cas,
                                       boolean lenient) throws SAXException, IOException {
            try {
                XmiSerializationSharedData shared = SHARED.get();
                XmiCasDeserializer.deserialize(in, cas, lenient, shared);
            } finally {
                // Avoid retaining ThreadLocal state on short-lived virtual threads.
                if (Thread.currentThread().isVirtual()) {
                    SHARED.remove();
                }
            }
        }
    }

    public static final class XmiLoggingErrorHandler implements ErrorHandler {
        private final DUUILogger logger;
        private final DUUIDocumentReader reader;
        private final DUUIDocument document;

        public XmiLoggingErrorHandler(DUUILogger logger, DUUIDocumentReader reader, DUUIDocument document) {
            this.logger = logger;
            this.reader = reader;
            this.document = document;
        }

        @Override
        public void warning(SAXParseException e) {
            logger.warn(
                DUUIContexts.reader(reader, document).status(DUUIStatus.SERIALIZE),
                "XMI serialization warning for %s: %s",
                document.getPath(),
                e.getMessage()
            );
        }

        @Override
        public void error(SAXParseException e) throws SAXException {
            logger.warn(
                DUUIContexts.reader(reader, document)
                    .exception(e)
                    .status(DUUIStatus.SERIALIZE),
                "Non-fatal XMI serialization error for %s: %s",
                document.getPath(),
                e.getMessage()
            );
            throw e; // keep behavior: fail on error
        }

        @Override
        public void fatalError(SAXParseException e) throws SAXException {
            logger.error(
                DUUIContexts.reader(reader, document)
                        .exception(e)
                        .status(DUUIStatus.FAILED),
                String.format(
                    "Fatal XMI serialization error for %s: %s",
                    document.getPath(),
                    e.getMessage()
                )
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
                if (outputExtension.equalsIgnoreCase(Mime.EXT_GZ)) {
                    compressorStream = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.GZIP, outputStream);
                } else if (outputExtension.equalsIgnoreCase(Mime.EXT_XZ)) {
                    compressorStream = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.XZ, outputStream);
                } else if (outputExtension.equalsIgnoreCase(Mime.EXT_BZ2)) {
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
                    // best-effort close; nothing we can do here
                }
            }
        }

        return outputStream.toByteArray();
    }

}
