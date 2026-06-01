package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.DUUIFrameworkStateException;
import org.texttechnologylab.duui.orchestration.worker.DUUIPlatformExecutorService;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.orchestration.worker.DUUIVirtualExecutorService;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.timelines.DUUIPhaseAspect;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

            executor.submit(task, DUUIDispatchPolicy.of(org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode.CPU, 1));

            assertEquals("PLATFORM:runtime-platform", task.await());
        }
    }

    @Test
    void phaseAspectSchedulesFromMethodAnnotations() throws Exception {
        DUUIPhaseAspect aspect = new DUUIPhaseAspect(new DUUIDispatcher());

        assertTrue(aspect.around(this, annotatedIoPhase(), List.of(), () -> Thread.currentThread().isVirtual()));
        assertFalse(aspect.around(this, annotatedCpuPhase(), List.of(), () -> Thread.currentThread().isVirtual()));
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

    @Phase(value = DUUIStatus.SERIALIZE, dispatch = DUUIDispatchMode.IO)
    public void ioPhase() {
    }

    @Phase(value = DUUIStatus.DESERIALIZE, dispatch = DUUIDispatchMode.CPU)
    public void cpuPhase() {
    }

    private static Method annotatedIoPhase() throws NoSuchMethodException {
        Method method = DUUIReworkRuntimeTest.class.getDeclaredMethod("ioPhase");
        method.setAccessible(true);
        return method;
    }

    private static Method annotatedCpuPhase() throws NoSuchMethodException {
        Method method = DUUIReworkRuntimeTest.class.getDeclaredMethod("cpuPhase");
        method.setAccessible(true);
        return method;
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
