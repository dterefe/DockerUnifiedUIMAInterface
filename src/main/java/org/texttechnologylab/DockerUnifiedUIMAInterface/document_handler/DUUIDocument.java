package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import org.apache.uima.cas.Marker;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILoggers;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DUUIDocument {

    private static final DUUILogger logger = DUUILoggers.getLogger(DUUIDocument.class);
    private String name;
    /**
     * The absolute path to the document including the name.
     */
    private String path;
    private long size;
    private byte[] bytes;
    private final AtomicInteger progess = new AtomicInteger(0);
    private String status = DUUIStatus.WAITING;
    private String error;
    private boolean isFinished = false;
    private long durationDecode = 0L;
    private long durationDeserialize = 0L;
    private long durationWait = 0L;
    private long durationProcess = 0L;
    private long startedAt = 0L;
    private long finishedAt = 0L;
    private long uploadProgress = 0L;
    private long downloadProgress = 0L;
    private final Map<String, Integer> annotations = new HashMap<>();
    private final Map<String, AnnotationRecord> annotationRecords = new HashMap<>();
    private Marker marker = null;

    public DUUIDocument(String name, String path, long size) {
        this.name = name;
        this.path = path;
        this.size = size;
    }

    public DUUIDocument(String name, String path) {
        this.name = name;
        this.path = path;
    }

    public DUUIDocument(String name, String path, byte[] bytes) {
        this.name = name;
        this.path = path;
        this.bytes = bytes;
        this.size = bytes.length;
    }

    public DUUIDocument(String name, String path, JCas jCas) {
        if (jCas.getDocumentText() != null) {
            this.bytes = jCas.getDocumentText().getBytes(StandardCharsets.UTF_8);
        }
        else if (jCas.getSofaDataStream() != null) {
            try {
                this.bytes = jCas.getSofaDataStream().readAllBytes();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.name = name;
        this.path = path;
        this.size = bytes.length;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof DUUIDocument)) {
            return false;
        }

        DUUIDocument _o = (DUUIDocument) o;
        return _o.getPath().equals(getPath());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Retrieve the documents file extension.
     *
     * @return The file extension including the dot character. E.G. '.txt'.
     */
    public String getFileExtension() {
        int extensionStartIndex = name.lastIndexOf('.');
        if (extensionStartIndex == -1) return "";
        return name.substring(extensionStartIndex);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
        if (bytes.length > 0) this.size = bytes.length;
    }

    /**
     * Convert the bytes into a ByteArrayInputStream for processing.
     *
     * @return A new {@link ByteArrayInputStream} containing the content of the document.
     */
    public ByteArrayInputStream toInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Convert the bytes into a String.
     *
     * @return A new String containing the content of the document.
     */
    public String getText() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Increment the document progress by one.
     */
    public void incrementProgress() {
        progess.incrementAndGet();
    }

    public AtomicInteger getProgess() {
        return progess;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    public long getDurationDecode() {
        return durationDecode;
    }

    public void setDurationDecode(long durationDecode) {
        this.durationDecode = durationDecode;
    }

    public long getDurationDeserialize() {
        return durationDeserialize;
    }

    public void setDurationDeserialize(long durationDeserialize) {
        this.durationDeserialize = durationDeserialize;
    }

    public long getDurationWait() {
        return durationWait;
    }

    public void setDurationWait(long durationWait) {
        this.durationWait = durationWait;
    }

    public long getDurationProcess() {
        return durationProcess;
    }

    public void setDurationProcess(long durationProcess) {
        this.durationProcess = durationProcess;
    }

    public long getStartedAt() {
        return startedAt;
    }

    /**
     * Utility method to set the startedAt timestamp to the current time.
     */
    public void setStartedAt() {
        startedAt = Instant.now().toEpochMilli();
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getFinishedAt() {
        return finishedAt;
    }

    /**
     * Utility method to set the finishedAt timestamp to the current time.
     */
    public void setFinishedAt() {
        finishedAt = Instant.now().toEpochMilli();
    }

    public void setFinishedAt(long finishedAt) {
        this.finishedAt = finishedAt;
    }

    private AnnotationState state(TOP top) {
        return marker.isNew(top) 
            ? AnnotationState.NEW : marker.isModified(top) 
            ? AnnotationState.MODIFIED :
            AnnotationState.BASELINE;
    }

    private AnnotationRecord merge(AnnotationRecord r1, AnnotationRecord r2) {
        return r1 != null && r2 != null ? 
            new AnnotationRecord(compare(r1.state(), r2.state()), r1.count() + r2.count()) : 
            r1 == null ? r2 : r1; 
    }

    private AnnotationState compare(AnnotationState s1, AnnotationState s2) {
        return s1 != null && s2 != null
            ? s1 == AnnotationState.NEW       ? s1
            : s2 == AnnotationState.NEW       ? s2
            : s1 == AnnotationState.MODIFIED  ? s1
            : s2 == AnnotationState.MODIFIED  ? s2
            : AnnotationState.BASELINE
            : s1 == null ? s2 : s1;
    }

    public void countAnnotations(JCas cas) {

        annotations.clear();
        annotationRecords.clear();

        boolean isMarkerValid = ensureValidMarker(cas);

        JCasUtil.select(cas, TOP.class)
            .stream()
            .collect(
                () -> new HashMap<String, AnnotationRecord>(),
                (map, fs) -> {
                    map.merge(
                        fs.getType().getName(), 
                        new AnnotationRecord(state(fs), 1),
                        this::merge
                    );
                },
                (left, right) -> right.forEach((type, rec) ->
                    left.merge(
                        type,
                        rec,
                        this::merge
                    )
                )
            )
            .forEach((typeName, rec) -> {
                annotations.put(typeName, rec.count());
                if (isMarkerValid) {
                    annotationRecords.put(typeName, rec);
                }
            });
    }

    public void initializeMarker(JCas cas) {
        CASImpl casImpl = (CASImpl) cas.getCas();
        if (casImpl.getCurrentMark() != null) {
            logger.warn(
                DUUIEvent.Context.document(
                    getPath(),
                    DUUIStatus.ACTIVE
                ),
                "Marker already initialized for CAS of document %s, cannot track new annotations",
                getPath()
            );
            return;
        }
        marker = casImpl.createMarker();
    }

    private boolean ensureValidMarker(JCas cas) {
        if (marker == null) {
            logger.warn(
                DUUIEvent.Context.document(
                    getPath(),
                    DUUIStatus.ACTIVE
                ),
                "No marker initialized for document %s",
                getPath()
            );
            return false;
        }
        CASImpl casImpl = (CASImpl) cas.getCas();
        Marker current = casImpl.getCurrentMark();
        if (current == null || current != marker) {
            logger.warn(
                DUUIEvent.Context.document(
                    getPath(),
                    DUUIStatus.ACTIVE
                ),
                "Marker for document %s is not current or has been cleared",
                getPath()
            );
            return false;
        }
        return true;
    }

    public Map<String, Integer> getAnnotations() {
        return annotations;
    }

    public Map<String, AnnotationRecord> getAnnotationRecords() {
        return annotationRecords;
    }

    public long getUploadProgress() {
        return uploadProgress;
    }

    public void setUploadProgress(long uploadProgress) {
        this.uploadProgress = uploadProgress;
    }

    public long getDownloadProgress() {
        return downloadProgress;
    }

    public void setDownloadProgress(long downloadProgress) {
        this.downloadProgress = downloadProgress;
    }

    public enum AnnotationState {
        BASELINE,
        NEW,
        MODIFIED
    }

    public record AnnotationRecord(AnnotationState state, int count) { }
}
