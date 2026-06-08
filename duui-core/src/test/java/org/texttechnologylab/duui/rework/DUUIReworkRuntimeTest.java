package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.DUUIFrameworkStateException;
import org.texttechnologylab.duui.orchestration.worker.DUUIPlatformExecutor;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.orchestration.worker.DUUIVirtualExecutorService;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;

import java.util.concurrent.atomic.AtomicReference;
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
        try (DUUIExecutor executor = DUUIExecutor.getInstance("runtime-platform")) {
            DUUITask<String> task = executor.task(new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertNotNull(worker.requireCurrentTask());
                return worker.environment().name() + ":" + worker.orchestratorId();
            });

            executor.submit(task, DUUIDispatchPolicy.of(org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode.CPU, 1));

            assertEquals("PLATFORM:runtime-platform", task.await());
        }
    }

    @Test
    void platformWorkerPersistsWhileTaskBindingChanges() {
        DUUIPlatformExecutor service = new DUUIPlatformExecutor("runtime-persistent", DUUIWorker.Type.PIPELINE, 1);
        try {
            AtomicReference<String> firstTaskId = new AtomicReference<>();
            AtomicReference<String> secondTaskId = new AtomicReference<>();
            DUUITask<String> first = new DUUITask<>("runtime-persistent", new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertEquals(firstTaskId.get(), worker.requireCurrentTask().id());
                return worker.id();
            });
            DUUITask<String> second = new DUUITask<>("runtime-persistent", new DUUIExecutionContext(), () -> {
                DUUIWorker worker = DUUIWorker.current();
                assertEquals(secondTaskId.get(), worker.requireCurrentTask().id());
                return worker.id();
            });
            firstTaskId.set(first.id());
            secondTaskId.set(second.id());

            service.execute(first);
            service.execute(second);

            assertEquals(first.await(), second.await());
        } finally {
            service.shutdownNow();
        }
    }

    @Test
    void currentWorkerWorksInsideManagedVirtualTaskAndUnregisters() throws Exception {
        DUUIVirtualExecutorService service = new DUUIVirtualExecutorService("runtime-virtual", DUUIWorker.Type.PIPELINE);
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
