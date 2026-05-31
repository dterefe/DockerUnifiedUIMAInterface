package org.texttechnologylab.duui.dua.projection;

public final class DUAProjectionTypes {
    public static final String UCE_CORPUS_TYPE_NAME = "org.texttechnologylab.annotation.uce.UCECorpus";
    public static final String UCE_DOCUMENT_TYPE_NAME = "org.texttechnologylab.annotation.uce.UCEDocument";

    public static final DUAProjectionType<Corpus> CORPUS =
            DUAProjectionType.of(UCE_CORPUS_TYPE_NAME, Corpus.class);
    public static final DUAProjectionType<Document> DOCUMENT =
            DUAProjectionType.of(UCE_DOCUMENT_TYPE_NAME, Document.class);

    private DUAProjectionTypes() {
    }

    public static DUAProjectionType<Corpus> corpus() {
        return CORPUS;
    }

    public static DUAProjectionType<Document> document() {
        return DOCUMENT;
    }

    public static <T> DUAProjectionType<T> type(String typeName, Class<T> markerClass) {
        return DUAProjectionType.of(typeName, markerClass);
    }

    public interface Corpus {
    }

    public interface Document {
    }
}
