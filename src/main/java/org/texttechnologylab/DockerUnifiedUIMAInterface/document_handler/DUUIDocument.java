package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import org.apache.uima.cas.Marker;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext.DocumentContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContexts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIState;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILoggers;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIProcess;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;

public class DUUIDocument {

    private static final DUUILogger logger = DUUILoggers.getLogger(DUUIDocument.class);
    private final DocumentState state = new DocumentState();
    private Marker marker = null;

    /**
     * Single source of truth for all mutable document state and metrics.
     * The public API of {@link DUUIDocument} delegates to this state object.
     */
    public static final class DocumentState extends DUUIState {
        private String name;
        /**
         * The absolute path to the document including the name.
         */
        private String path;
        private long size;
        private byte[] bytes;
        private final AtomicInteger progess = new AtomicInteger(0);
        private long durationDecode = 0L;
        private long durationDeserialize = 0L;
        private long durationWait = 0L;
        private long durationProcess = 0L;
        private long uploadProgress = 0L;
        private long downloadProgress = 0L;
        private boolean isFinished = false;
        private long startedAt = 0L;
        private long finishedAt = 0L;
        private String error;
        private final Map<String, Integer> annotations = new HashMap<>();
        private final Map<String, AnnotationRecord> annotationRecords = new HashMap<>();
        private Map<Long, DUUIEvent> events = new ConcurrentHashMap<>();

        private static final DUUIContext.Payload EMPTY_PAYLOAD = new DUUIContext.Payload(
            DUUIStatus.UNKNOWN,
            "",
            DUUIContext.PayloadKind.NONE,
            ""
        );

        public static final class ComponentState {
            private volatile boolean segmented = false;
            private volatile DUUIContext.Payload errorPayload = EMPTY_PAYLOAD;
            private final AtomicLong waitMillis = new AtomicLong(0);
            private final AtomicLong serializeMillis = new AtomicLong(0);
            private final AtomicLong processMillis = new AtomicLong(0);
            private final AtomicLong deserializeMillis = new AtomicLong(0);
            private final AtomicLong luaProcessMillis = new AtomicLong(0);

            public boolean isSegmented() {
                return segmented;
            }

            public DUUIContext.Payload getErrorPayload() {
                return errorPayload;
            }

            public long getWaitMillis() { return waitMillis.get(); }
            public long getSerializeMillis() { return serializeMillis.get(); }
            public long getProcessMillis() { return processMillis.get(); }
            public long getDeserializeMillis() { return deserializeMillis.get(); }
            public long getLuaProcessMillis() { return luaProcessMillis.get(); }

            public Document toDocument() {
                return new Document("is_segmented", this.segmented)
                    .append("payload", DUUIContexts.toDocument(this.errorPayload))
                    .append("duration_wait", this.waitMillis.get())
                    .append("duration_serialize", this.serializeMillis.get())
                    .append("duration_process", this.processMillis.get())
                    .append("duration_deserialize", this.deserializeMillis.get())
                    .append("duration_lua_process", this.luaProcessMillis.get());
            }
        }

        /**
         * Per-component document state (segmentation, last error payload, phase durations).
         * Keyed by component id.
         */
        private final Map<String, ComponentState> components = new ConcurrentHashMap<>();

        public Document toDocument() {
            Document out = super.toDocument()
                .append("name", name)
                .append("path", path)
                .append("size", size)
                .append("status", status == null ? DUUIStatus.UNKNOWN.value() : status.value())
                .append("progress", progess.get())
                .append("duration_decode", durationDecode)
                .append("duration_deserialize", durationDeserialize)
                .append("duration_wait", durationWait)
                .append("duration_process", durationProcess)
                .append("progress_upload", uploadProgress)
                .append("progress_download", downloadProgress)
                .append("is_finished", isFinished)
                .append("started_at", startedAt)
                .append("finished_at", finishedAt)
                .append("last_updated_at", lastUpdatedAt)
                .append("last_event_id", lastEventId)
                .append("events", events.values().stream().map(DUUIEvent::getId).toList())
                .append("error", error);

                if (isFinished) {
                    out.append("annotations", annotations);
                    out.append("annotations_new", annotationRecords);
                }
                
                out.append("components", 
                    components.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey,
                    e -> e.getValue().toDocument()
                )));

            return out;
        }

        public void setLastUpdatedAt(long timestampMillis) {
            this.lastUpdatedAt = timestampMillis;
        }

        public void setLastEventId(long id) {
            this.lastEventId = id;
        }

        public void update(DUUIEvent event, DocumentContext ctx) {
            DUUIStatus next = ctx.status();
            if (DUUIProcess.isEffectivelyActive(next)) {
                next = DUUIStatus.PROCESS;
            }
            transitionStatus(next, event.getTimestamp());
            this.lastEventId = event.getId();
            this.lastUpdatedAt = event.getTimestamp();
            this.thread = ctx.thread();

            events.put(event.getId(), event);
        }
    }

    public DUUIDocument(String name, String path, long size) {
        state.name = name;
        state.path = path;
        state.size = size;
        long now = Instant.now().toEpochMilli();
        state.initStatus(DUUIStatus.WAITING, now);
        touch(now);
    }

    public DUUIDocument(String name, String path) {
        state.name = name;
        state.path = path;
        long now = Instant.now().toEpochMilli();
        state.initStatus(DUUIStatus.WAITING, now);
        touch(now);
    }

    public DUUIDocument(String name, String path, byte[] bytes) {
        state.name = name;
        state.path = path;
        state.bytes = bytes;
        state.size = bytes.length;
        long now = Instant.now().toEpochMilli();
        state.initStatus(DUUIStatus.WAITING, now);
        touch(now);
    }

    public DUUIDocument(String name, String path, JCas jCas) {
        if (jCas.getDocumentText() != null) {
            state.bytes = jCas.getDocumentText().getBytes(StandardCharsets.UTF_8);
        }
        else if (jCas.getSofaDataStream() != null) {
            try {
                state.bytes = jCas.getSofaDataStream().readAllBytes();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        state.name = name;
        state.path = path;
        state.size = state.bytes == null ? 0 : state.bytes.length;
        long now = Instant.now().toEpochMilli();
        state.initStatus(DUUIStatus.WAITING, now);
        touch(now);
    }

    public DocumentState getState() {
        return state;
    }

    /**
     * Marks the state as updated "now".
     */
    public void touch() {
        touch(Instant.now().toEpochMilli());
    }

    /**
     * Marks the state as updated at the given timestamp (epoch millis).
     */
    public void touch(long timestampMillis) {
        state.setLastUpdatedAt(timestampMillis);
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
        return state.name;
    }

    public void setName(String name) {
        state.name = name;
        touch();
    }

    /**
     * Retrieve the documents file extension.
     *
     * @return The file extension including the dot character. E.G. '.txt'.
     */
    public String getFileExtension() {
        int extensionStartIndex = state.name.lastIndexOf('.');
        if (extensionStartIndex == -1) return "";
        return state.name.substring(extensionStartIndex);
    }

    public String getPath() {
        return state.path;
    }

    public void setPath(String path) {
        state.path = path;
        touch();
    }

    public long getSize() {
        return state.size;
    }

    public void setSize(long size) {
        state.size = size;
        touch();
    }

    public byte[] getBytes() {
        return state.bytes;
    }

    public void setBytes(byte[] bytes) {
        state.bytes = bytes;
        if (bytes.length > 0) state.size = bytes.length;
        touch();
    }

    /**
     * Convert the bytes into a ByteArrayInputStream for processing.
     *
     * @return A new {@link ByteArrayInputStream} containing the content of the document.
     */
    public ByteArrayInputStream toInputStream() {
        return new ByteArrayInputStream(state.bytes);
    }

    /**
     * Convert the bytes into a String.
     *
     * @return A new String containing the content of the document.
     */
    public String getText() {
        return new String(state.bytes, StandardCharsets.UTF_8);
    }

    /**
     * Increment the document progress by one.
     */
    public void incrementProgress() {
        state.progess.incrementAndGet();
        touch();
    }

    public AtomicInteger getProgess() {
        return state.progess;
    }

    public String getStatus() {
        return state.getStatus().toString();
    }

    public DUUIStatus getStatusEnum() {
        return state.getStatus();
    }

    /**
     * @deprecated Use {@link #setStatus(DUUIStatus)} instead.
     */
    @Deprecated
    public void setStatus(String status) {
        setStatus(DUUIStatus.fromString(status));
    }

    public void setStatus(DUUIStatus status) {
        long now = Instant.now().toEpochMilli();
        state.transitionStatus(status, now);
        touch(now);
    }

    public String getError() {
        return state.error;
    }

    public void setError(String error) {
        setError(DUUILogContext.getContext(), error);
    }
    public void setError(DUUIContext ctx, String error) {
        logger.error(
            ctx,
            error
        );
        setStatus(DUUIStatus.FAILED);
        state.error = error;
        touch();
    }

    public boolean isFinished() {
        return state.isFinished;
    }

    public void setFinished(boolean finished) {
        state.isFinished = finished;
        touch();
    }

    public long getDurationDecode() {
        return state.durationDecode;
    }

    public void setDurationDecode(long durationDecode) {
        state.durationDecode = durationDecode;
        touch();
    }

    public long getDurationDeserialize() {
        return state.durationDeserialize;
    }

    public void setDurationDeserialize(long durationDeserialize) {
        state.durationDeserialize = durationDeserialize;
        touch();
    }

    public long getDurationWait() {
        return state.durationWait;
    }

    public void setDurationWait(long durationWait) {
        state.durationWait = durationWait;
        touch();
    }

    public long getDurationProcess() {
        return state.durationProcess;
    }

    public void setDurationProcess(long durationProcess) {
        state.durationProcess = durationProcess;
        touch();
    }

    public Map<String, DocumentState.ComponentState> getComponentStates() {
        return state.components;
    }

    public Map<String, DUUIContext.Payload> getComponentErrorPayloads() {
        Map<String, DUUIContext.Payload> mapped = new HashMap<>();
        for (Map.Entry<String, DocumentState.ComponentState> entry : state.components.entrySet()) {
            mapped.put(entry.getKey(), entry.getValue().getErrorPayload());
        }
        return mapped;
    }

    public void setComponentErrorPayload(String componentId, DUUIContext.Payload payload) {
        if (componentId == null || componentId.isEmpty() || payload == null) return;
        DocumentState.ComponentState comp = state.components.computeIfAbsent(
            componentId,
            _ignored -> new DocumentState.ComponentState()
        );
        comp.errorPayload = payload;
        touch();
    }

    public void setComponentSegmented(String componentId, boolean segmented) {
        if (componentId == null || componentId.isEmpty()) return;
        DocumentState.ComponentState comp = state.components.computeIfAbsent(
            componentId,
            _ignored -> new DocumentState.ComponentState()
        );
        comp.segmented = segmented;
        touch();
    }

    public void addComponentPhaseDurationMillis(String componentId, DUUIStatus phase, long millis) {
        if (componentId == null || componentId.isEmpty() || phase == null || millis < 0) {
            return;
        }

        DocumentState.ComponentState comp = state.components.computeIfAbsent(
            componentId,
            _ignored -> new DocumentState.ComponentState()
        );
        boolean segmented = comp.segmented;

        switch (phase) {
            case COMPONENT_WAIT -> {
                if (segmented) comp.waitMillis.addAndGet(millis); else comp.waitMillis.set(millis);
            }
            case COMPONENT_SERIALIZE -> {
                if (segmented) comp.serializeMillis.addAndGet(millis); else comp.serializeMillis.set(millis);
            }
            case COMPONENT_PROCESS -> {
                if (segmented) comp.processMillis.addAndGet(millis); else comp.processMillis.set(millis);
            }
            case COMPONENT_DESERIALIZE -> {
                if (segmented) comp.deserializeMillis.addAndGet(millis); else comp.deserializeMillis.set(millis);
            }
            case COMPONENT_LUA_PROCESS -> {
                if (segmented) comp.luaProcessMillis.addAndGet(millis); else comp.luaProcessMillis.set(millis);
            }
            default -> {
                // ignore other statuses
            }
        }
        touch();
    }

    public long getStartedAt() {
        return state.startedAt;
    }

    /**
     * Utility method to set the startedAt timestamp to the current time.
     */
    public void setStartedAt() {
        state.startedAt = Instant.now().toEpochMilli();
        touch();
    }

    public void setStartedAt(long startedAt) {
        state.startedAt = startedAt;
        touch();
    }

    public long getFinishedAt() {
        return state.finishedAt;
    }

    /**
     * Utility method to set the finishedAt timestamp to the current time.
     */
    public void setFinishedAt() {
        state.finishedAt = Instant.now().toEpochMilli();
        touch();
    }

    public void setFinishedAt(long finishedAt) {
        state.finishedAt = finishedAt;
        touch();
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

        state.annotations.clear();
        state.annotationRecords.clear();

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
                state.annotations.put(typeName, rec.count());
                if (isMarkerValid) {
                    state.annotationRecords.put(typeName, rec);
                }
            });
    }

    public void initializeMarker(JCas cas) {
        CASImpl casImpl = (CASImpl) cas.getCas();
        if (casImpl.getCurrentMark() != null) {
            logger.warn(
                DUUIContexts.doc(this).status(DUUIStatus.WAITING),
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
                DUUIContexts.doc(this).status(DUUIStatus.WAITING),
                "No marker initialized for document %s",
                getPath()
            );
            return false;
        }
        CASImpl casImpl = (CASImpl) cas.getCas();
        Marker current = casImpl.getCurrentMark();
        if (current == null || current != marker) {
            logger.warn(
                DUUIContexts.doc(this).status(DUUIStatus.WAITING),
                "Marker for document %s is not current or has been cleared",
                getPath()
            );
            return false;
        }
        return true;
    }

    public Map<String, Integer> getAnnotations() {
        return state.annotations;
    }

    public Map<String, AnnotationRecord> getAnnotationRecords() {
        return state.annotationRecords;
    }

    public long getUploadProgress() {
        return state.uploadProgress;
    }

    public void setUploadProgress(long uploadProgress) {
        state.uploadProgress = uploadProgress;
        touch();
    }

    public long getDownloadProgress() {
        return state.downloadProgress;
    }

    public void setDownloadProgress(long downloadProgress) {
        state.downloadProgress = downloadProgress;
        touch();
    }

    public enum AnnotationState {
        BASELINE,
        NEW,
        MODIFIED
    }

    public record AnnotationRecord(AnnotationState state, int count) { }
}
