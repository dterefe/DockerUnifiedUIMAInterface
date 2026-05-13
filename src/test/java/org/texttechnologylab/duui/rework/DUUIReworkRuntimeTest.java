package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.orchestration.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.DUUIFrameworkStateException;
import org.texttechnologylab.duui.orchestration.DUUIPlatformExecutorService;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.orchestration.DUUIVirtualExecutorService;
import org.texttechnologylab.duui.orchestration.DUUIWorker;
import org.texttechnologylab.duui.pipeline.DUUIExecutor;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIReworkRuntimeTest {
    @Test
    void currentWorkerFailsOutsideManagedExecution() {
        assertThrows(DUUIFrameworkStateException.class, DUUIWorker::current);
    }

    @Test
    void currentWorkerWorksInsideManagedPlatformTask() {
        try (DUUIExecutor executor = new DUUIExecutor("runtime-platform")) {
            DUUITask<String> task = executor.task(new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertNotNull(worker.requireCurrentTask());
                return worker.kind().name() + ":" + worker.orchestratorId();
            });

            executor.submit(task, DUUIDispatchPolicy.of(org.texttechnologylab.duui.orchestration.DUUIDispatchMode.CPU, 1));

            assertEquals("PLATFORM:runtime-platform", task.await());
        }
    }

    @Test
    void platformWorkerPersistsWhileTaskBindingChanges() {
        DUUIPlatformExecutorService service = new DUUIPlatformExecutorService("runtime-persistent", 1);
        try {
            DUUITask<String> first = new DUUITask<>("runtime-persistent", new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertEquals(firstTaskId(), worker.requireCurrentTask().id());
                return worker.id();
            });
            DUUITask<String> second = new DUUITask<>("runtime-persistent", new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertEquals(secondTaskId(), worker.requireCurrentTask().id());
                return worker.id();
            });
            firstTaskId = first.id();
            secondTaskId = second.id();

            service.execute(first);
            service.execute(second);

            assertEquals(first.await(), second.await());
        } finally {
            service.shutdownNow();
        }
    }

    private static String firstTaskId;
    private static String secondTaskId;

    private static String firstTaskId() {
        return firstTaskId;
    }

    private static String secondTaskId() {
        return secondTaskId;
    }

    @Test
    void currentWorkerWorksInsideManagedVirtualTaskAndUnregisters() throws Exception {
        DUUIVirtualExecutorService service = new DUUIVirtualExecutorService("runtime-virtual");
        try {
            DUUITask<Long> task = new DUUITask<>("runtime-virtual", new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertNotNull(worker.requireCurrentTask());
                return worker.threadId();
            });

            service.execute(task);

            assertTrue(task.await(5, TimeUnit.SECONDS) > 0);
        } finally {
            service.shutdownNow();
        }
    }
}
