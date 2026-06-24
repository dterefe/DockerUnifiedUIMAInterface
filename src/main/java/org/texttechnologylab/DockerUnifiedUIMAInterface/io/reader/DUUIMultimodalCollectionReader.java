package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.utilities.helper.StringUtils;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class DUUIMultimodalCollectionReader implements DUUICollectionReader {

    private String _path;
    private ConcurrentLinkedQueue<String> _filePaths;
    private final ConcurrentLinkedQueue<String> _filePathsBackup;
    private final ConcurrentLinkedQueue<ByteReadFuture> _loadedFiles;

    private final String _viewName;

    private final int _initialSize;
    private final AtomicInteger _docNumber;
    private final long _maxMemory;
    private final AtomicLong _currentMemorySize;

    private boolean _addMetadata = true;

    private final String _targetPath = null;

    private String _language = null;

    private AdvancedProgressMeter progress = null;

    private int debugCount = 25;

    private String targetLocation = null;

    public DUUIMultimodalCollectionReader(String folder, String ending) {
        this(folder, ending, "_InitialView", -1, 25, getRandomFromMode(null, -1), getSortFromMode(null), "", true, null, 0, "", null);
    }

    public DUUIMultimodalCollectionReader(String folder, String ending, String viewName) {
        this(folder, ending, viewName, -1, 25, getRandomFromMode(null, -1), getSortFromMode(null), "", true, null, 0, "", null);
    }

    public DUUIMultimodalCollectionReader(String folder, String ending, int maxFiles) {
        this(folder, ending, "_InitialView", maxFiles, 25, getRandomFromMode(null, -1), getSortFromMode(null), "", true, null, 0, "", null);
    }

    public DUUIMultimodalCollectionReader(String folder, String ending, String viewName, int maxFiles, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles, String targetLocation, String targetEnding) {
        this.targetLocation = targetLocation;
        _addMetadata = bAddMetadata;
        _language = language;
        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _filePathsBackup = new ConcurrentLinkedQueue<>();
        _viewName = viewName;

        if (new File(savePath).exists() && savePath.length() > 0) {
            File sPath = new File(savePath);

            String sContent = null;
            try {
                sContent = StringUtils.getContent(sPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] sSplit = sContent.split("\n");

            Collections.addAll(_filePaths, sSplit);
        } else {
            File fl = new File(folder);
            if (!fl.isDirectory()) {
                throw new RuntimeException("The folder is not a directory!");
            }

            AtomicInteger addedCount = new AtomicInteger(0);

            _path = folder;
            addFilesToConcurrentList(fl, ending, _filePaths, maxFiles, addedCount);

            if (skipSmallerFiles > 0) {
                _filePaths = skipBySize(_filePaths, skipSmallerFiles);
            }
        }


        if (skipSmallerFiles > 0) {
            _filePaths = skipBySize(_filePaths, skipSmallerFiles);
        }

        if (bSort) {
            _filePaths = sortBySize(_filePaths);
        }

        if (bSort && iRandom > 0) {
            System.out.println("Sorting and Random Selection is active, using the " + (iRandom > 0 ? "largest " : "smallest ") + Math.abs(iRandom) + " documents.");
//            _filePaths = takeFirstOrLast(_filePaths, iRandom);
        } else if (iRandom > 0) {
            _filePaths = random(_filePaths, iRandom);
        }

        if (savePath.length() > 0) {
            File nFile = new File(savePath);

            if (!nFile.exists()) {
                StringBuilder sb = new StringBuilder();
                _filePaths.forEach(f -> {
                    if (sb.length() > 0) {
                        sb.append("\n");
                    }
                    sb.append(f);
                });
                try {
                    StringUtils.writeContent(sb.toString(), nFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // remove files that are already in the target location
        // NOTE we do this after saving the file list, as we do not want to change anything but only avoid processing files multiple times
        if (this.targetLocation != null) {
            // _filePaths = removeIfInTarget(_filePaths, this.targetLocation, targetEnding, this._path, ending);
        }

        _filePathsBackup.addAll(_filePaths);

        this.debugCount = debugCount;

        System.out.printf("Found %d files matching the pattern! \t Using Random: %d\n", _filePaths.size(), iRandom);
        _initialSize = _filePaths.size();
        _docNumber = new AtomicInteger(0);
        _currentMemorySize = new AtomicLong(0);
        // 500 MB
        _maxMemory = 500 * 1024 * 1024;

        progress = new AdvancedProgressMeter(_initialSize);
    }

    private static int getRandomFromMode(AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, int sampleSize) {
        if (sampleMode == AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.SMALLEST) {
            return sampleSize * -1;
        }
        return sampleSize;
    }

    private static boolean getSortFromMode(AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode) {
        return mode != AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.RANDOM;
    }

    public static void addFilesToConcurrentList(File folder, String ending, ConcurrentLinkedQueue<String> paths, int maxFiles, AtomicInteger addedCount) {
        if (maxFiles > 0 && addedCount.get() >= maxFiles) {
            return;
        }

        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return;
        }

        for (File file : listOfFiles) {
            if (maxFiles > 0 && addedCount.get() >= maxFiles) {
                return;
            }

            if (file.isFile()) {
                if (file.getName().endsWith(ending)) {
                    paths.add(file.getPath());
                    addedCount.incrementAndGet();
                }
            } else if (file.isDirectory()) {
                addFilesToConcurrentList(file, ending, paths, maxFiles, addedCount);
            }
        }

    }

    public static ConcurrentLinkedQueue<String> sortBySize(ConcurrentLinkedQueue<String> paths) {

        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<String>();

        rQueue.addAll(paths.stream().sorted((s1, s2) -> {
            Long firstLength = new File(s1).length();
            Long secondLength = new File(s2).length();

            return firstLength.compareTo(secondLength) * -1;
        }).collect(Collectors.toList()));

        return rQueue;

    }

    /**
     * Skips files smaller than skipSmallerFiles
     *
     * @param paths            paths to files
     * @param skipSmallerFiles skip files smaller than this value in bytes
     * @return filtered paths to files
     */
    public static ConcurrentLinkedQueue<String> skipBySize(ConcurrentLinkedQueue<String> paths, int skipSmallerFiles) {
        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<>();

        System.out.println("Skip files smaller than " + skipSmallerFiles + " bytes");
        System.out.println("  Number of files before skipping: " + paths.size());

        rQueue.addAll(paths
                .stream()
                .filter(s -> new File(s).length() >= skipSmallerFiles)
                .collect(Collectors.toList())
        );

        System.out.println("  Number of files after skipping: " + rQueue.size());

        return rQueue;
    }

    public static ConcurrentLinkedQueue<String> random(ConcurrentLinkedQueue<String> paths, int iRandom) {

        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<String>();

        Random nRandom = new Random(iRandom);

        ArrayList<String> sList = new ArrayList<>();
        sList.addAll(paths);

        Collections.shuffle(sList, nRandom);

        if (iRandom > sList.size()) {
            rQueue.addAll(sList.subList(0, sList.size()));
        } else {
            rQueue.addAll(sList.subList(0, iRandom));
        }


        return rQueue;

    }


    public static String getSize(String sPath) {
        return FileUtils.byteCountToDisplaySize(new File(sPath).length());
    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return this.progress;
    }

    @Override
    public void getNextCas(JCas empty) {
        ByteReadFuture future = _loadedFiles.poll();

        byte[] bFile = null;
        String result = null;
        if (future == null) {
            result = _filePaths.poll();
            if (result == null) return;
        } else {
            result = future.getPath();
            bFile = future.getBytes();
            long factor = 1;
            if (result.endsWith(".gz") || result.endsWith(".xz")) {
                factor = 10;
            }
            _currentMemorySize.getAndAdd(-factor * (long) bFile.length);
        }
        int val = _docNumber.addAndGet(1);

        progress.setDone(val);
        progress.setLeft(_initialSize - val);

        if (_initialSize - progress.getCount() > debugCount) {
            if (val % debugCount == 0 || val == 0) {
                System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
            }
        } else {
            System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
        }

        if (bFile == null) {
            try {
                bFile = Files.readAllBytes(Path.of(result));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {

            empty.reset();

            JCas mView;
            try {
                mView = empty.getView(_viewName);

            } catch (Exception e) {
                mView = empty.createView(_viewName);
            }

            var parts = result.split("\\.");
            String fileExtension = parts[parts.length - 1];

            File fFile = new File(result);
            String mimeType = Files.probeContentType(fFile.toPath());


            if (mimeType == null) {
                if (fileExtension.equals("xmi")) {
                    mimeType = "application/xmi";
                }
            }

            System.out.println(mimeType);

            String sofaString = "";

            switch (mimeType.split("/")[0]) {
                case "image":
                case "video":
                case "audio":
                    sofaString = Base64.encodeBase64String(FileUtils.readFileToByteArray(fFile));
                    mView.setSofaDataString(sofaString, mimeType);
                    break;
                case "text":
                    sofaString = readFile(fFile);
                    mView.setSofaDataString(sofaString, mimeType);

                    break;
                case "application":

                    if (fileExtension.equals("xmi")) {
                        InputStream decodedFile = new ByteArrayInputStream(Files.readAllBytes(fFile.toPath()));
                        XmiCasDeserializer.deserialize(decodedFile, mView.getCas(), true);
                        break;
                    } else if (mimeType.split("/")[1].equals("x-gzip") || mimeType.split("/")[1].equals("gzip")) {
                        //CompressorInputStream decodedFile = new CompressorStreamFactory(true).createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(Files.readAllBytes(fFile.toPath())));
                        //XmiCasDeserializer.deserialize(decodedFile, mView.getCas(), true);
                        GzJsonReader.ParsedArticle r = GzJsonReader.readSingleJsonFromGz(fFile.toPath());
                        mView.setSofaDataString(r.getText(),"text/plain");


                        break;
                    } else if (mimeType.split("/")[1].equals("x-xz")) {
                        CompressorInputStream decodedFile = new CompressorStreamFactory(true).createCompressorInputStream(CompressorStreamFactory.XZ, new ByteArrayInputStream(Files.readAllBytes(fFile.toPath())));
                        XmiCasDeserializer.deserialize(decodedFile, mView.getCas(), true);
                        break;
                    }

                    sofaString = Base64.encodeBase64String(FileUtils.readFileToByteArray(fFile));
                    System.out.println(sofaString.substring(0, 150));
                    mView.setSofaDataString(sofaString, mimeType);
                    break;
                default:
                    try {
                        sofaString = readFile(fFile);
                    } catch (Exception e) {
                        sofaString = Base64.encodeBase64String(FileUtils.readFileToByteArray(fFile));
                    }
                    mView.setSofaDataString(sofaString, mimeType);
                    break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        if (_addMetadata) {
            if (JCasUtil.select(empty, DocumentMetaData.class).size() == 0) {
                DocumentMetaData dmd = DocumentMetaData.create(empty);
                File pFile = new File(result);
                dmd.setDocumentId(pFile.getName());
                dmd.setDocumentTitle(pFile.getName());
                dmd.setDocumentUri(pFile.getAbsolutePath());
                dmd.addToIndexes();
            }
        }

        if (_language != null && !_language.isEmpty()) {
            empty.setDocumentLanguage(_language);
        }

    }

    public void reset() {
        _filePaths = _filePathsBackup;
        _docNumber.set(0);
        progress = new AdvancedProgressMeter(_initialSize);
    }

    @Override
    public boolean hasNext() {
        return _filePaths.size() > 0;
    }

    @Override
    public long getSize() {
        return _filePaths.size();
    }

    public CompletableFuture<Integer> getAsyncNextByteArray() throws IOException, CompressorException, SAXException {
        String result = _filePaths.poll();
        if (result == null) return CompletableFuture.completedFuture(1);
        CompletableFuture<Integer> val = AsyncFiles
                .readAllBytes(Paths.get(result), 1024 * 1024 * 5)
                .thenApply(bytes -> {
                    _loadedFiles.add(new ByteReadFuture(result, bytes));

                    //Calculate estimated unpacked size by using a compression ratio of 0.1
                    long factor = 1;
                    if (result.endsWith(".gz") || result.endsWith(".xz")) {
                        factor = 10;
                    }
                    _currentMemorySize.getAndAdd(factor * (long) bytes.length);
                    return 0;
                });
        return val;
    }

    @Override
    public long getDone() {
        return _docNumber.get();
    }

    public String formatSize(long lSize) {

        int u = 0;
        for (; lSize > 1024 * 1024; lSize >>= 10) {
            u++;
        }
        if (lSize > 1024)
            u++;
        return String.format("%.1f %cB", lSize / 1024f, " kMGTPE".charAt(u));

    }

    public enum DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE {
        RANDOM,
        SMALLEST,
        LARGEST
    }

    private String readFile(File file) throws FileNotFoundException {
        String result = "";
        Scanner myReader = new Scanner(file);
        while (myReader.hasNextLine()) {
            if (result == "") {
                result = myReader.nextLine();
            } else {
                result += "\n" + myReader.nextLine();
            }
        }

        return result;
    }

    class ByteReadFuture {
        private final String _path;
        private final byte[] _bytes;

        public ByteReadFuture(String path, byte[] bytes) {
            _path = path;
            _bytes = bytes;
        }

        public String getPath() {
            return _path;
        }

        public byte[] getBytes() {
            return _bytes;
        }
    }
}

class GzJsonReader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class ParsedArticle {
        private final String extractedFileName;
        private final String url;
        private final String text;

        public ParsedArticle(String extractedFileName, String url, String text) {
            this.extractedFileName = extractedFileName;
            this.url = url;
            this.text = text;
        }

        public String getExtractedFileName() {
            return extractedFileName;
        }

        public String getUrl() {
            return url;
        }

        public String getText() {
            return text;
        }
    }

    public static ParsedArticle readSingleJsonFromGz(Path gzFile) throws IOException {
        String extractedFileName = deriveExtractedFileName(gzFile);

        try (InputStream fileIn = Files.newInputStream(gzFile);
             InputStream gzipIn = new GZIPInputStream(fileIn)) {

            JsonNode root = MAPPER.readTree(gzipIn);

            String url = root.path("url").asText("");

            StringBuilder textBuilder = new StringBuilder();
            JsonNode article = root.path("article");

            if (article.isArray()) {
                for (JsonNode item : article) {
                    if ("text".equals(item.path("type").asText())) {
                        collectText(item.path("text"), textBuilder);
                    }
                }
            }

            return new ParsedArticle(extractedFileName, url, textBuilder.toString().trim());
        }
    }

    private static void collectText(JsonNode node, StringBuilder sb) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return;
        }

        if (node.isTextual()) {
            appendWithSpace(sb, node.asText());
            return;
        }

        if (node.isArray()) {
            for (JsonNode child : node) {
                if ("text".equals(child.path("type").asText())) {
                    collectText(child.path("text"), sb);
                }
            }
            return;
        }

        if (node.isObject()) {
            if ("text".equals(node.path("type").asText())) {
                collectText(node.path("text"), sb);
            }
        }
    }

    private static void appendWithSpace(StringBuilder sb, String text) {
        if (text == null) {
            return;
        }

        String cleaned = text.trim();
        if (cleaned.isEmpty()) {
            return;
        }

        if (sb.length() > 0) {
            sb.append(' ');
        }
        sb.append(cleaned);
    }

    private static String deriveExtractedFileName(Path gzFile) {
        String fileName = gzFile.getFileName().toString();
        if (fileName.endsWith(".gz")) {
            return fileName.substring(0, fileName.length() - 3);
        }
        return fileName;
    }
}