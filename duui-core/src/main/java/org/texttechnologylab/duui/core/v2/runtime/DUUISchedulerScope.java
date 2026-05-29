package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.orchestration.scheduling.DUUIDirector;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.scheduling.DUUITraitDirector;

public final class DUUISchedulerScope implements AutoCloseable {
    private final DUUISystemScope system;
    private DUUIScheduler scheduler = new DUUIScheduler();
    private DUUIDirector director = new DUUITraitDirector();
    private boolean closed;

    DUUISchedulerScope(DUUISystemScope system) {
        this.system = system;
    }

    public DUUISchedulerScope sequential() {
        this.scheduler = new DUUIScheduler();
        return this;
    }

    public DUUISchedulerScope scheduler(DUUIScheduler scheduler) {
        this.scheduler = scheduler == null ? new DUUIScheduler() : scheduler;
        return this;
    }

    public DUUISchedulerScope director(DUUIDirector director) {
        this.director = director == null ? new DUUITraitDirector() : director;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        system.scheduler(scheduler);
        system.director(director);
    }
}
