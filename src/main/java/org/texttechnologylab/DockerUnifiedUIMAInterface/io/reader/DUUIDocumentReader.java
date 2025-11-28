package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XMLSerializer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUILocalDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIDocumentDecoder;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.Timer;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class DUUIDocumentReader implements DUUICollectionReader {

    private final long maximumMemory;
    private final AtomicInteger progress;
    private final AtomicLong currentMemorySize = new AtomicLong(0);
    private final int initialSize;
    private ConcurrentLinkedQueue<DUUIDocument> documentQueue;
    private final ConcurrentLinkedQueue<DUUIDocument> documentsBackup;
    private final ConcurrentLinkedQueue<DUUIDocument> loadedDocuments;
    private List<DUUIDocument> preProcessor;
    private final Builder builder;
    private final DUUIComposer composer;
    private final int initial;
    private final int skipped;

    private DUUIDocumentReader(Builder builder) {
        this.builder = builder;
        this.composer = builder.composer;

        // restoreFromSavePath(); This needs clarification.
        try {
            preProcessor = builder
                .inputHandler
                .listDocuments(
                    builder.inputPaths,
                    builder.inputFileExtension,
                    builder.recursive);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        initial = preProcessor.size();

        if (builder.minimumDocumentSize > 0) removeSmallFiles();
        if (builder.sortBySize) sortFilesAscending();
        if (builder.checkTarget) removeDocumentsInTarget();

        documentQueue = new ConcurrentLinkedQueue<>(preProcessor);
        documentsBackup = new ConcurrentLinkedQueue<>(preProcessor);
        loadedDocuments = new ConcurrentLinkedQueue<>();

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format("Processing %d files.", documentQueue.size())
        );

        initialSize = documentQueue.size();
        progress = new AtomicInteger(0);
        currentMemorySize.set(0);
        maximumMemory = 500 * 1024 * 1024;

        composer.addDocuments(preProcessor);
        skipped = initial - documentQueue.size();
    }

    @Override
    public void shutdown() {
        builder.inputHandler.shutdown();
        if (builder.inputHandler == builder.outputHandler) builder.outputHandler.shutdown();
    }

    public static Builder builder(DUUIComposer composer) {
        return new Builder(composer);
    }

    /**
     * Adds all unprocessed documents at the specified savePath to the documents Queue.
     */
    private void restoreFromSavePath() {
        if (builder.savePath == null || builder.savePath.isEmpty())
            preProcessor = new ArrayList<>();

        DUUILocalDocumentHandler handler = new DUUILocalDocumentHandler();
        try {
            preProcessor = handler.listDocuments(builder.savePath, builder.outputFileExtension);
        } catch (IOException exception) {
            preProcessor = new ArrayList<>();
        }
    }

    private void removeSmallFiles() {
        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format("Skip files smaller than %d bytes.", builder.minimumDocumentSize));

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format("Number of files before skipping %d.", preProcessor.size()));

        preProcessor = preProcessor
            .stream()
            .filter(document -> document.getSize() >= builder.minimumDocumentSize)
            .collect(Collectors.toList());

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format("Number of files after skipping %d.", preProcessor.size()));
    }

    private void sortFilesAscending() {
        preProcessor = preProcessor
            .stream()
            .sorted(Comparator.comparingLong(DUUIDocument::getSize))
            .collect(Collectors.toList());

        composer.addEvent(
            DUUIEvent.Sender.READER,
            "Sorted files by size in ascending order"
        );
    }

    private void removeDocumentsInTarget() {
        if (builder.outputHandler == null) return;

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format("Checking output location %s for existing documents.", builder.outputPath));


        List<DUUIDocument> documentsInTarget;

        try {
            documentsInTarget = builder
                .outputHandler
                .listDocuments(
                    builder.outputPath,
                    builder.outputFileExtension,
                    builder.recursive);
        } catch (IOException e) {
            return;
        }


        if (documentsInTarget.isEmpty()) {
            composer.addEvent(
                DUUIEvent.Sender.READER,
                "Found 0 documents in output location. Keeping all files from input location.");
            return;
        }

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Found %d documents in output location. Checking against %d documents in input location.",
                documentsInTarget.size(), preProcessor.size()));


        Set<String> existingFiles = documentsInTarget
            .stream()
            .map(DUUIDocument::getPath)
            .map(path -> path.replaceAll(builder.outputFileExtension, ""))
            .map(path -> path.replaceAll(builder.inputFileExtension, ""))
            .map(path -> Paths.get(path).getFileName().toString())
            .collect(Collectors.toSet());

        int removedCounter = 0;

        List<DUUIDocument> preProcessorCopy = new ArrayList<>(preProcessor);

        for (DUUIDocument document : preProcessorCopy) {
            String stem = document
                .getName()
                .replaceAll(builder.inputFileExtension, "")
                .replaceAll(builder.outputFileExtension, "");

            if (!existingFiles.contains(stem)) continue;
            preProcessor.remove(document);
            removedCounter++;
        }

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Removed %d documents from input location that are already present in output location. Keeping %d",
                removedCounter, preProcessor.size()));
    }

    public long getMaximumMemory() {
        return maximumMemory;
    }

    public long getCurrentMemorySize() {
        return currentMemorySize.get();
    }

    public void reset() {
        documentQueue = documentsBackup;
        progress.set(0);
    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return null;
    }

    public DUUIDocument getNextDocument(JCas pCas) {
        if (composer.shouldShutdown()) {
            return null;
        }

        DUUIDocument document = pollDocument();
        if (document == null) return null;
        document.setStartedAt();

        Timer timer = new Timer();

        timer.start();
        InputStream decodedDocument = decodeDocument(document, timer);
        timer.stop();

        document.setDurationDecode(timer.getDuration());

        document.setStatus(DUUIStatus.DESERIALIZE);

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Document %s decoded after %d ms",
                document.getPath(),
                timer.getDuration())
        );

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Deserializing document %s",
                document.getPath())
        );

        timer.restart();

        if (decodedDocument != null) {
            try {
                XmiCasDeserializer.deserialize(decodedDocument, pCas.getCas(), true);
            } catch (Exception e) {
                composer.addEvent(
                    DUUIEvent.Sender.DOCUMENT,
                    String.format(
                        "Failed to deserialize XMI for document %s, falling back to plain text: %s",
                        document.getPath(),
                        e.toString()
                    ),
                    DUUIComposer.DebugLevel.ERROR
                );
                pCas.setDocumentText(document.getText().trim());
            }
        } else {
            composer.addEvent(
                DUUIEvent.Sender.DOCUMENT,
                String.format(
                    "Decoded content for document %s is unavailable, using raw text representation if present.",
                    document.getPath()
                ),
                DUUIComposer.DebugLevel.ERROR
            );
            pCas.setDocumentText(document.getText().trim());
        }

        timer.stop();
        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Document %s deserialized after %d ms",
                document.getPath(),
                timer.getDuration())
        );

        document.setDurationDeserialize(timer.getDuration());
        document.setStatus(DUUIStatus.WAITING);

        if (builder.addMetadata) {
            if (JCasUtil.select(pCas, DocumentMetaData.class).isEmpty()) {
                DocumentMetaData metadata = DocumentMetaData.create(pCas);
                metadata.setDocumentId(document.getName());
                metadata.setDocumentTitle(document.getName());
                metadata.setDocumentUri(document.getPath());
                metadata.addToIndexes();
            } else {
                DocumentMetaData metaData = JCasUtil.selectSingle(pCas, DocumentMetaData.class);
                metaData.setDocumentUri(document.getPath());
                metaData.addToIndexes();
            }
        }

        if (builder.language != null && !builder.language.isEmpty()) {
            pCas.setDocumentLanguage(builder.language);
        }

        return document;
    }

    @Override
    public void getNextCas(JCas pCas) {
        if (composer.shouldShutdown()) {
            return;
        }

        DUUIDocument document = pollDocument();
        if (document == null) return;
        document.setStartedAt();

        Timer timer = new Timer();

        timer.start();
        InputStream decodedDocument = decodeDocument(document, timer);
        timer.stop();

        document.setDurationDecode(timer.getDuration());

        document.setStatus(DUUIStatus.DESERIALIZE);

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Document %s decoded after %d ms",
                document.getPath(),
                timer.getDuration())
        );

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Deserializing document %s",
                document.getPath())
        );

        timer.restart();

        if (decodedDocument != null) {
            try {
                XmiCasDeserializer.deserialize(decodedDocument, pCas.getCas(), true);
            } catch (Exception e) {
                composer.addEvent(
                    DUUIEvent.Sender.DOCUMENT,
                    String.format(
                        "Failed to deserialize XMI for document %s, falling back to plain text: %s",
                        document.getPath(),
                        e.toString()
                    ),
                    DUUIComposer.DebugLevel.ERROR
                );
                pCas.setDocumentText(document.getText().trim());
            }
        } else {
            composer.addEvent(
                DUUIEvent.Sender.DOCUMENT,
                String.format(
                    "Decoded content for document %s is unavailable, using raw text representation if present.",
                    document.getPath()
                ),
                DUUIComposer.DebugLevel.ERROR
            );
            pCas.setDocumentText(document.getText().trim());
        }

        timer.stop();
        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Document %s deserialized after %d ms",
                document.getPath(),
                timer.getDuration())
        );

        document.setDurationDeserialize(timer.getDuration());
        document.setStatus(DUUIStatus.WAITING);

        if (builder.addMetadata) {
            if (JCasUtil.select(pCas, DocumentMetaData.class).isEmpty()) {
                DocumentMetaData metadata = DocumentMetaData.create(pCas);
                metadata.setDocumentId(document.getName());
                metadata.setDocumentTitle(document.getName());
                metadata.setDocumentUri(document.getPath());
                metadata.addToIndexes();
            } else {
                DocumentMetaData metaData = JCasUtil.selectSingle(pCas, DocumentMetaData.class);
                metaData.setDocumentUri(document.getPath());
                metaData.addToIndexes();
            }
        }

        if (builder.language != null && !builder.language.isEmpty()) {
            pCas.setDocumentLanguage(builder.language);
        }
    }

    private DUUIDocument pollDocument() {
        DUUIDocument polled = loadedDocuments.poll();
        DUUIDocument document;

        if (polled == null) {
            document = documentQueue.poll();
            if (document == null) return null;
        } else {
            document = polled;
            long factor = 1;
            if (document.getName().endsWith(".gz") || document.getName().endsWith(".xz")) {
                factor = 10;
            }
            currentMemorySize.getAndAdd(-factor * document.getSize());
        }

        this.progress.addAndGet(1);

        if (polled == null) {
            try {
                polled = builder.inputHandler.readDocument(document.getPath());
            } catch (IOException exception) {
                composer.addEvent(
                    DUUIEvent.Sender.READER,
                    String.format(
                        "Failed to read document %s: %s",
                        document.getPath(),
                        exception.toString()
                    ),
                    DUUIComposer.DebugLevel.ERROR
                );
                throw new RuntimeException(exception);
            }
        }


        document = composer.addDocument(polled);
        document.setBytes(polled.getBytes());
        return document;
    }

    private InputStream decodeDocument(DUUIDocument document, Timer timer) {
        document.setStatus(DUUIStatus.DECODE);

        composer.addEvent(
            DUUIEvent.Sender.READER,
            String.format(
                "Decoding document %s",
                document.getPath())
        );

        timer.start();

        InputStream decodedFile;

        try {
            decodedFile = DUUIDocumentDecoder.decode(document);
        } catch (IOException e) {
            composer.addEvent(
                DUUIEvent.Sender.DOCUMENT,
                String.format(
                    "Failed to decode document %s: %s",
                    document.getPath(),
                    e.toString()
                ),
                DUUIComposer.DebugLevel.ERROR
            );
            document.setError(String.format(
                "%s%n%s",
                e.getClass().getCanonicalName(),
                e.getMessage() == null ? "" : e.getMessage()));
            document.setStatus(DUUIStatus.FAILED);
            return null;
        }
        timer.stop();
        return decodedFile;
    }

    public CompletableFuture<Integer> getAsyncNextByteArray() {
        DUUIDocument document = documentQueue.poll();
        if (document == null) return CompletableFuture.completedFuture(1);

        return CompletableFuture.supplyAsync(
            () -> {
                try {
                    return builder.inputHandler.readDocument(document.getPath());
                } catch (IOException e) {
                    composer.addEvent(
                        DUUIEvent.Sender.READER,
                        String.format(
                            "Failed to read document %s asynchronously: %s",
                            document.getPath(),
                            e.toString()
                        ),
                        DUUIComposer.DebugLevel.ERROR
                    );
                    throw new RuntimeException(e);
                }
            }
        ).thenApply(_document -> {
            loadedDocuments.add(_document);
            long factor = 1;
            if (_document.getName().endsWith(".gz") || _document.getName().endsWith(".xz")) {
                factor = 10;
            }
            currentMemorySize.getAndAdd(factor * document.getSize());
            return 0;
        });
    }

    @Override
    public boolean hasNext() {
        return progress.get() < initialSize;
    }

    @Override
    public long getSize() {
        return documentQueue.size();
    }

    @Override
    public long getDone() {
        return progress.get();
    }

    public int getInitial() {
        return initial;
    }

    public int getSkipped() {
        return skipped;
    }

    public boolean hasOutput() {
        return builder.outputHandler != null;
    }

    public void upload(DUUIDocument document, JCas cas) throws IOException, SAXException {
        if (builder.outputHandler == null || document.getUploadProgress() != 0) return;

        composer.addEvent(DUUIEvent.Sender.READER, String.format("Uploading document %s", document.getPath()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
        XMLSerializer sax2xml = new XMLSerializer(outputStream);

        // Use XMI 1.1 in case of special characters which break serialization in XMI 1.0.
        // sax2xml.setOutputProperty(javax.xml.transform.OutputKeys.VERSION, "1.1");

        xmiCasSerializer.serialize(cas.getCas(), sax2xml.getContentHandler(), null, null, null);

        String inputExtension = document.getFileExtension();
        String outputExtension = builder.outputFileExtension;

        String originalName = document.getName();
        String outputName = originalName;

        if (originalName != null && !originalName.isEmpty()) {
            String inExt = inputExtension == null ? "" : inputExtension;
            String outExt = outputExtension == null ? "" : outputExtension;

            // If it already ends with the desired extension, keep it.
            if (!outExt.isEmpty() && originalName.endsWith(outExt)) {
                outputName = originalName;
            }
            // If it ends with the input extension, swap just that suffix.
            else if (!inExt.isEmpty() && originalName.endsWith(inExt)) {
                outputName =
                    originalName.substring(0, originalName.length() - inExt.length()) + outExt;
            }
            // Fallback: replace the last extension segment, if any.
            else if (!outExt.isEmpty()) {
                int dotIndex = originalName.lastIndexOf('.');
                if (dotIndex > 0) {
                    outputName = originalName.substring(0, dotIndex) + outExt;
                }
            }
        }

        byte[] payload = outputStream.toByteArray();

        // Compress payload depending on desired output extension
        if (outputExtension != null) {
            try {
                if (outputExtension.equalsIgnoreCase(".gz")) {
                    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                    try (CompressorOutputStream cos = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.GZIP, compressed)) {
                        cos.write(payload);
                    }
                    payload = compressed.toByteArray();
                } else if (outputExtension.equalsIgnoreCase(".xz")) {
                    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                    try (CompressorOutputStream cos = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.XZ, compressed)) {
                        cos.write(payload);
                    }
                    payload = compressed.toByteArray();
                } else if (outputExtension.equalsIgnoreCase(".bz2")) {
                    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                    try (CompressorOutputStream cos = new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.BZIP2, compressed)) {
                        cos.write(payload);
                    }
                    payload = compressed.toByteArray();
                }
            } catch (CompressorException e) {
                throw new IOException(
                    String.format("Failed to compress document %s as %s", document.getPath(), outputExtension),
                    e
                );
            }
        }

        DUUIDocument temp = new DUUIDocument(
            outputName,
            builder.outputPath + "/" + outputName,
            payload
        );

        builder
            .outputHandler
            .writeDocument(temp, builder.outputPath);

        long sizeStore = document.getSize();
        document.setBytes(new byte[]{});
        document.setSize(sizeStore);

        document.setUploadProgress(temp.getUploadProgress());
        document.setStatus(DUUIStatus.COMPLETED);
    }

    public static final class Builder {
        private final DUUIComposer composer;
        private List<String> inputPaths;
        private String inputFileExtension;
        private IDUUIDocumentHandler inputHandler;
        private String outputPath;
        private String outputFileExtension;
        private IDUUIDocumentHandler outputHandler;

        /**
         * Processed documents are stored here. If an error occurs during processing, only files not present
         * at this path will be processed on restart.
         */
        private String savePath = "/temp/duui";
        private String language;
        private long minimumDocumentSize = 0L;
        private boolean sortBySize = false;
        private boolean addMetadata = true;
        private boolean checkTarget = false;
        private boolean recursive = false;

        public Builder(DUUIComposer composer) {
            this.composer = composer;
        }

        public DUUIDocumentReader build() {
            return new DUUIDocumentReader(this);
        }

        public Builder withInputPath(String inputPath) {
            this.inputPaths = Collections.singletonList(inputPath);
            return this;
        }

        public Builder withInputPaths(List<String> inputPaths) {
            this.inputPaths = inputPaths;
            return this;
        }

        public Builder withInputFileExtension(String inputFileExtension) {
            this.inputFileExtension = inputFileExtension;
            return this;
        }

        public Builder withInputHandler(IDUUIDocumentHandler inputHandler) {
            this.inputHandler = inputHandler;
            return this;
        }

        public Builder withOutputPath(String outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        public Builder withOutputFileExtension(String outputFileExtension) {
            this.outputFileExtension = outputFileExtension;
            return this;
        }

        public Builder withOutputHandler(IDUUIDocumentHandler outputHandler) {
            this.outputHandler = outputHandler;
            return this;
        }

        public Builder withSavePath(String savePath) {
            this.savePath = savePath;
            return this;
        }

        public Builder withLanguage(String language) {
            this.language = language;
            return this;
        }

        public Builder withMinimumDocumentSize(long minimumDocumentSize) {
            this.minimumDocumentSize = minimumDocumentSize;
            return this;
        }

        public Builder withSortBySize(boolean sortBySize) {
            this.sortBySize = sortBySize;
            return this;
        }

        public Builder withAddMetadata(boolean addMetadata) {
            this.addMetadata = addMetadata;
            return this;
        }

        public Builder withCheckTarget(boolean checkTarget) {
            this.checkTarget = checkTarget;
            return this;
        }

        public Builder withRecursive(boolean recursive) {
            this.recursive = recursive;
            return this;
        }

    }

    public static List<DUUIDocument> loadDocumentsFromPath(String path, String fileExtension, boolean recursive) throws IOException {
        DUUILocalDocumentHandler handler = new DUUILocalDocumentHandler();
        return handler.readDocuments(
            handler
                .listDocuments(path, fileExtension, recursive)
                .stream()
                .map(DUUIDocument::getPath)
                .collect(Collectors.toList())
        );
    }
}
