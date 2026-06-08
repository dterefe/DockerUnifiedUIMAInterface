package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.event.DUUIEventLevel;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIEventStatus;
import org.texttechnologylab.duui.event.DUUIEventType;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.event.DUUIJulHandler;
import org.texttechnologylab.duui.event.DUUILogger;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIEventTest {
    @Test
    void eventsAreEnrichedFromCurrentTaskContext() {
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService service = new DUUIEventService(List.of(sink));
        DUUIExecutionContext context = new DUUIExecutionContext().eventService(service);

        try (DUUIExecutor executor = DUUIExecutor.getInstance("event-orchestrator")) {
            DUUITask<Void> task = executor.task(context, () -> {
                context.eventContext(context.eventContext().toBuilder()
                        .artifactId("artifact-1")
                        .checkpointId("checkpoint-1")
                        .stageId("stage-1")
                        .componentId("component-1")
                        .nodeId("node-1")
                        .annotatorId("annotator-1")
                        .build());
                service.logger("test").info("hello");
                return null;
            });

            executor.submit(task, DUUIDispatchPolicy.of(DUUIDispatchMode.CPU, 1));
            task.await();
        }

        assertEquals(1, sink.events().size());
        var event = sink.events().get(0);
        assertEquals(DUUIEventType.LOG, event.type());
        assertEquals("event-orchestrator", event.orchestratorId());
        assertNotNull(event.taskId());
        assertEquals("artifact-1", event.artifactId());
        assertEquals("checkpoint-1", event.checkpointId());
        assertEquals("stage-1", event.stageId());
        assertEquals("component-1", event.componentId());
        assertEquals("node-1", event.nodeId());
        assertEquals("annotator-1", event.annotatorId());
        assertNotNull(event.workerId());
        assertEquals(32, event.traceId().length());
        assertEquals(16, event.spanId().length());
    }

    @Test
    void duuiLoggerAndJulHandlerEmitLogEvents() {
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService service = new DUUIEventService(List.of(sink));
        DUUILogger logger = service.logger("duui-test");

        logger.debug("debugged");
        DUUIJulHandler handler = new DUUIJulHandler(service);
        LogRecord record = new LogRecord(Level.INFO, "jul-info");
        record.setLoggerName("jul-test");
        handler.publish(record);

        assertEquals(2, sink.events().size());
        assertEquals(DUUIEventType.LOG, sink.events().get(0).type());
        assertEquals(DUUIEventLevel.DEBUG, sink.events().get(0).level());
        assertEquals("jul-test", sink.events().get(1).name());
        assertEquals(DUUIEventType.LOG, sink.events().get(1).type());
    }

    @Test
    void scopedEventsEmitStartedCompletedAndFailed() {
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService service = new DUUIEventService(List.of(sink));

        try (var ignored = service.scope("build")) {
            // scope emits lifecycle events
        }

        try {
            service.scoped("analysis", () -> {
                throw new IllegalStateException("broken");
            });
        } catch (Exception ignored) {
        }

        assertTrue(sink.events().stream().anyMatch(event -> event.status() == DUUIEventStatus.STARTED && "build".equals(event.name())));
        assertTrue(sink.events().stream().anyMatch(event -> event.status() == DUUIEventStatus.COMPLETED && "build".equals(event.name())));
        assertTrue(sink.events().stream().anyMatch(event -> event.status() == DUUIEventStatus.FAILED && "analysis".equals(event.name())));
        assertTrue(sink.events().stream().anyMatch(event -> event.type() == DUUIEventType.ERROR && "analysis".equals(event.name())));
    }
}
