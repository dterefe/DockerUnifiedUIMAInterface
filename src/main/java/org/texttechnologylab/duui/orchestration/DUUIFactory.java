package org.texttechnologylab.duui.orchestration;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DUUIFactory {
    private DUUIFactory() {}

    public static ThreadFactory platformThreadFactory(String orchestratorId) {
        AtomicInteger count = new AtomicInteger();
        return runnable -> Thread.ofPlatform()
                .name("duui-platform-" + count.incrementAndGet())
                .factory()
                .newThread(() -> {
                    DUUIWorkerRegistry.registerCurrentThread(orchestratorId, DUUIWorkerKind.PLATFORM, false);
                    runnable.run();
                });
    }

    public static ThreadFactory virtualThreadFactory(String orchestratorId) {
        AtomicInteger count = new AtomicInteger();
        return runnable -> Thread.ofVirtual()
                .name("duui-virtual-" + count.incrementAndGet())
                .factory()
                .newThread(() -> {
                    DUUIWorkerRegistry.registerCurrentThread(orchestratorId, DUUIWorkerKind.VIRTUAL, false);
                    try {
                        runnable.run();
                    } finally {
                        DUUIWorkerRegistry.unregisterCurrentThread();
                    }
                });
    }
}
