package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;

/**
 * Shared base state model for DUUI process tracking.
 * <p>
 * Used by both {@link org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIProcess}
 * and document-level state tracking ({@link org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument}).
 */
public abstract class DUUIState {

    protected volatile DUUIStatus status = DUUIStatus.UNKNOWN;
    protected volatile String thread = "";
    protected volatile long lastUpdatedAt = 0L;
    protected volatile long lastEventId = 0L;

    /**
     * Accumulated wall-clock duration per {@link DUUIStatus} (millis) for this state.
     * Updated when {@link #transitionStatus(DUUIStatus, long)} is used.
     */
    protected final Map<DUUIStatus, AtomicLong> statusDurationsMillis = new ConcurrentHashMap<>();
    protected volatile long statusSinceMillis = 0L;

    public DUUIStatus getStatus() {
        return status;
    }

    /**
     * Sets an initial status and starts duration tracking from {@code nowMillis}.
     * Does not accumulate any previous duration.
     */
    public void initStatus(DUUIStatus initialStatus, long nowMillis) {
        this.status = initialStatus == null ? DUUIStatus.UNKNOWN : initialStatus;
        this.statusSinceMillis = nowMillis;
    }

    /**
     * Transitions to {@code nextStatus} and accumulates the time spent in the previous {@link #getStatus()}.
     */
    public void transitionStatus(DUUIStatus nextStatus, long nowMillis) {
        if (nowMillis > 0L && statusSinceMillis > 0L) {
            long delta = nowMillis - statusSinceMillis;
            if (delta > 0L) {
                DUUIStatus previous = this.status == null ? DUUIStatus.UNKNOWN : this.status;
                statusDurationsMillis.computeIfAbsent(previous, _k -> new AtomicLong(0)).addAndGet(delta);
            }
        }
        this.status = nextStatus == null ? DUUIStatus.UNKNOWN : nextStatus;
        this.statusSinceMillis = nowMillis;
    }

    public void transitionStatus(DUUIStatus nextStatus) {
        transitionStatus(nextStatus, Instant.now().toEpochMilli());
    }

    public Map<DUUIStatus, AtomicLong> getStatusDurationsMillis() {
        return statusDurationsMillis;
    }

    public long getStatusDurationMillis(DUUIStatus status) {
        if (status == null) return 0L;
        AtomicLong value = statusDurationsMillis.get(status);
        return value == null ? 0L : value.get();
    }

    public long getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public long getLastEventId() {
        return lastEventId;
    }

    public String thread() { return thread; }

    public Document toDocument() {
        Document out = new Document("status", status == null ? DUUIStatus.UNKNOWN.value() : status.value())
            .append("last_updated_at", lastUpdatedAt)
            .append("thread", thread)
            .append("last_event_id", lastEventId);

        Document statusDurations = new Document();
        for (Map.Entry<DUUIStatus, AtomicLong> e : statusDurationsMillis.entrySet()) {
            DUUIStatus k = e.getKey();
            AtomicLong v = e.getValue();
            statusDurations.append(k == null ? DUUIStatus.UNKNOWN.value() : k.value(), v == null ? 0L : v.get());
        }
        out.append("status_durations", statusDurations);
        return out;
    }
}
